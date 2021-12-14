package com.wowza.wms.plugin.avmix;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.stonesoup.RabbitSingleton;
import com.stonesoup.switching.MessageType;
import com.stonesoup.switching.StreamConfirmData;
import com.stonesoup.switching.StreamConfirmMessage;
import com.stonesoup.switching.StreamCreatorConsumer;
import com.wowza.wms.amf.AMFPacket;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.stream.IMediaStreamMetaDataProvider;
import com.wowza.wms.stream.publish.PlaylistItem;
import com.wowza.wms.stream.publish.Publisher;
import com.wowza.wms.stream.publish.Stream;
import com.wowza.wms.vhost.IVHost;

public class OutputStream extends Thread {
	
	private static final Class<OutputStream> CLASS = OutputStream.class;
	private static final String CLASS_NAME = CLASS.getName();
	
	private IApplicationInstance appInstance;
	private WMSLogger logger;
	private boolean doQuit;
	private boolean running = false;
	
	private String outputName;
	private volatile String audioName;
	private volatile String videoName;
	private int sleepTime = 125;
	private int idleShutdown = 2000;
	private int idleTime = 0;
	
	private Publisher publisher = null;
	private Queue<AMFPacket> packets = new PriorityQueue<AMFPacket>(16, new PacketComparator());
	private IMediaStream audioSource;
	private IMediaStream videoSource;
	private IMediaStream nxtVideoSource;
	private long audioSeq = -1;
	private long videoSeq = -1;
	private long switchTC = Long.MAX_VALUE;
	private long audioTC = 0;
	private long videoTC = 0;
	private long videoTCOffset = 0;
	private long audioTCOffset = 0;
	
	private final Object lock = new Object();
	
	private ObjectMapper json = new ObjectMapper();
	private String routingKeyTemplate;
	private String exchangeName;
	private String serverName;
	private String vHostName;
	
	private long maxSentAudioTC = Long.MIN_VALUE;
	private long maxSentVideoTC = Long.MIN_VALUE;
	
	// constructor has more fields for backwards compatibility with the original Wowza AVMixer class
	public OutputStream(IApplicationInstance appInstance, String outputName, long startTime, long sortDelay, boolean useOriginalTimecodes) {
		this.appInstance = appInstance;
		this.outputName = outputName;
		
		exchangeName = appInstance.getProperties().getPropertyStr("confirmationExchangeName");
		routingKeyTemplate = appInstance.getProperties().getPropertyStr("confirmationRoutingKeyTemplate");
		serverName = appInstance.getApplication().getVHost().getProperty("serverName");
		vHostName = appInstance.getVHost().getName();
		
		logger = WMSLoggerFactory.getLogger(CLASS);
	}
	
	@Override
	public void run() {
		logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Starting to run");
		running = true;
		// avoid dirty shutdown due to error
		try {
			while (true) {
				// shut down if requested
				if (doQuit) {
					logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Shutting down as requested");
					shutDown();
					break;
				}
				
				// avoid changing stream names while running
				synchronized (lock) {
					// first treat the case where both audio and video come from the same stream
					if (audioName!=null && videoName!=null && audioName.equals(videoName)) {
						// logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] "+ ": Same video and audio on outputstream " + appInstance.getContextStr() + "/" + outputName);
						IMediaStream stream = appInstance.getStreams().getStream(videoName);
						if (stream == null) {
							audioSource = null;
							videoSource = null;
							logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Stream name " + videoName +  " not playing. Nothing to play on video or audio");
						} else {
							// if a different source then the present one was requested
							if ((stream!=videoSource || stream!=audioSource) && nxtVideoSource==null) {
								// get the most recent frame from the stream
								AMFPacket lastFrame = stream.getLastPacket();
								// if there's at least a frame in the intended source video
								if (lastFrame != null) {
									logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio-video source change requested");
									nxtVideoSource = stream;
									// remember the timecode in the stream where the switch was requested
									switchTC = lastFrame.getAbsTimecode();
								}
							}
							
							// if we are pending a switch
							if (nxtVideoSource != null) {
								// get the most recent keyframe from the next source
								AMFPacket nxtKeyFrame = nxtVideoSource.getLastKeyFrame();
								// if we don't have any keyframes in the video yet, there's nothing to do
								if (nxtKeyFrame != null) {
									// if the keyframe is after the switch timecode
									if (nxtKeyFrame.getAbsTimecode() > switchTC) {
										// do the switch
										audioSource = stream;
										videoSource = stream;
										// create a copy of the packet, so we don't change the its data
										AMFPacket packet = nxtKeyFrame.clone(true); 
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio-video source change performed");
										// compute the timecode difference
										videoTCOffset = videoTC - packet.getAbsTimecode() + 1;
										// if the video timecode would bring the audio back in time
										if (packet.getAbsTimecode() + videoTCOffset < audioTC) {
											// then move it ahead a bit
											videoTCOffset += audioTC - packet.getAbsTimecode() - videoTCOffset + 1;
										}
										audioTCOffset = videoTCOffset;
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Video timecode offset: " + videoTCOffset + ". Current video timecode: " + videoTC);
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio timecode offset: " + audioTCOffset + ". Current audio timecode: " + audioTC);
										// communicate the switch
										sendConfirmation(outputName, stream.getName(), stream.getName());
										// start playing both audio and video starting with the keyframe
										videoSeq = packet.getSeq();
										videoTC = packet.getAbsTimecode() + videoTCOffset;
										audioSeq = videoSeq;
										audioTC = videoTC;
										// send audio config packet
										AMFPacket configPacket = stream.getAudioCodecConfigPacket(packet.getAbsTimecode());
										if (configPacket != null) {
											AMFPacket newPacket = configPacket.clone(true);
											// set a timecode that doesn't interfere with others
											newPacket.setAbsTimecode(packet.getAbsTimecode());
											// update the packet's timecode
											newPacket.setAbsTimecode(configPacket.getAbsTimecode() + audioTCOffset);
											packets.add(newPacket);
										}
										// send video config packet
										configPacket = stream.getVideoCodecConfigPacket(packet.getAbsTimecode());
										if (configPacket != null) {
											AMFPacket newPacket = configPacket.clone(true);
											// set a timecode that doesn't interfere with others
											newPacket.setAbsTimecode(packet.getAbsTimecode() + 1);
											// update the packet's timecode
											newPacket.setAbsTimecode(newPacket.getAbsTimecode() + videoTCOffset);
											packets.add(newPacket);
										}
										// send metadata
										IMediaStreamMetaDataProvider mdProvider = nxtVideoSource.getMetaDataProvider();
										List<AMFPacket> metaData = new ArrayList<AMFPacket>();
										mdProvider.onStreamStart(metaData, packet.getAbsTimecode());
										metaData.forEach(mdPacket -> {
											// if the packet exists and has data
											if (mdPacket!=null && mdPacket.getSize()>0 && mdPacket.getData()!=null) {
												AMFPacket newPacket = mdPacket.clone(true);
												// set a timecode that doesn't interfere with others
												newPacket.setAbsTimecode(packet.getAbsTimecode() + 2);
												// update the packet's timecode
												newPacket.setAbsTimecode(newPacket.getAbsTimecode() + videoTCOffset);
												packets.add(newPacket);
											}
										});
										// update the keyframe packet's timecode, so that it doesn't interfere with others
										packet.setAbsTimecode(packet.getAbsTimecode() + videoTCOffset + 3);
										// add the keyframe packet
										packets.add(packet);
										// reset the switch variables
										nxtVideoSource = null;
										switchTC = Long.MAX_VALUE;
									}
								} else {
									logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Waiting for keyframe");
								}
							}
						}
					} else {
						// if we have a video stream name
						if (videoName != null) {
							// different streams
							IMediaStream videoStream = appInstance.getStreams().getStream(videoName);
							// if the source video stream is running
							if (videoStream == null) {
								videoSource = null;
								logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Stream name " + videoName +  " not playing. Nothing to play on video ");
							} else {
								// if a different source then the present one was requested
								if (videoStream!=videoSource && nxtVideoSource==null) {
									// get the most recent frame from the stream
									AMFPacket lastFrame = videoStream.getLastPacket();
									// if there's at least a frame in the intended source video
									if (lastFrame != null) {
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Video source change requested");
										nxtVideoSource = videoStream;
										// remember the timecode in the stream where the switch was requested
										switchTC = lastFrame.getAbsTimecode();
									}
								}
								// if we are pending a switch
								if (nxtVideoSource != null) {
									// get the most recent keyframe from the next source
									AMFPacket nxtKeyFrame = nxtVideoSource.getLastKeyFrame();
									// if the keyframe is after the switch timecode
									if (nxtKeyFrame!=null && nxtKeyFrame.getAbsTimecode() > switchTC) {
										// do the switch
										videoSource = videoStream;
										// create a copy of the packet, so we don't change the its data
										AMFPacket packet = nxtKeyFrame.clone(true); 
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Video source change performed");
										// compute the timecode difference
										videoTCOffset = videoTC - packet.getAbsTimecode() + 1;
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Video timecode offset: " + videoTCOffset + ". Current video timecode: " + videoTC);
										// it's possible even if we only switch video now, we end up playing the same stream
										// so we should syncronize the timecodes
										if (audioSource!= null && videoSource.getName().equals(audioSource.getName())) {
											audioTCOffset = videoTCOffset;
											logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio timecode offset: " + audioTCOffset + ". Current audio timecode: " + audioTC);
											
										}
										// communicate the switch
										sendConfirmation(outputName, videoStream.getName(), null);
										// start playing video starting with the keyframe
										videoSeq = packet.getSeq();
										videoTC = packet.getAbsTimecode() + videoTCOffset;
										// send video config packet
										AMFPacket configPacket = videoStream.getVideoCodecConfigPacket(packet.getAbsTimecode());
										if (configPacket != null) {
											// set a timecode that doesn't interfere with others
											configPacket.setAbsTimecode(packet.getAbsTimecode());
											// update the packet's timecode
											configPacket.setAbsTimecode(configPacket.getAbsTimecode() + videoTCOffset);
											packets.add(configPacket);
										}
										// send metadata
										IMediaStreamMetaDataProvider mdProvider = nxtVideoSource.getMetaDataProvider();
										List<AMFPacket> metaData = new ArrayList<AMFPacket>();
										mdProvider.onStreamStart(metaData, packet.getAbsTimecode());
										metaData.forEach(mdPacket -> {
											// if the packet exists and has data
											if (mdPacket!=null && mdPacket.getSize()>0 && mdPacket.getData()!=null) {
												AMFPacket newPacket = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, mdPacket.getData());
												// set a timecode that doesn't interfere with others
												newPacket.setAbsTimecode(packet.getAbsTimecode() + 1);
												// update the packet's timecode
												newPacket.setAbsTimecode(newPacket.getAbsTimecode() + videoTCOffset);
												packets.add(newPacket);
											}
										});
										// update the packet's timecode, so that it doesn't interfere with others
										packet.setAbsTimecode(packet.getAbsTimecode() + videoTCOffset + 2);
										// add the keyframe packet
										packets.add(packet);
										// reset the switch variables
										nxtVideoSource = null;
										switchTC = Long.MAX_VALUE;
									}
								}
							}
						}
						// if we have an audio stream name
						if (audioName != null) {
							// different streams
							IMediaStream audioStream = appInstance.getStreams().getStream(audioName);
							// if the source video stream is running
							if (audioStream == null) {
								audioSource = null;
								logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Stream name " + audioName +  " not playing. Nothing to play on audio");
							} else {
								// if a different source then the present one was requested
								if (audioStream != audioSource) {
									// get the most recent frame from the stream
									AMFPacket lastPacket = audioStream.getLastPacket();
									// if there's at least a packet in the intended source audio
									if (lastPacket != null) {
										// do the switch
										audioSource = audioStream;
										// create a copy of the packet, so we don't change the its data
										AMFPacket packet = lastPacket.clone(true); 
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio source change requested");
										// remember the timecode in the stream where the switch was requested
										packet.getAbsTimecode();
										// there are no keyframes in audio, so simply perform the switch
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio source change performed");
										// compute the timecode difference
										audioTCOffset = audioTC - packet.getAbsTimecode() + 1;
										logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio timecode offset: " + audioTCOffset);
										// it's possible even if we only switch audio now, we end up playing the same stream
										// so we should syncronize the timecodes
										if (videoSource!=null && videoSource.getName().equals(audioSource.getName())) {
											videoTCOffset = audioTCOffset;
											logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Video timecode offset: " + videoTCOffset);
											
										}
										// communicate the switch
										sendConfirmation(outputName, null, audioStream.getName());
										// start playing audio from the current frame
										audioSeq = packet.getSeq();
										audioTC = packet.getAbsTimecode() + audioTCOffset;
										// send video config packet
										AMFPacket configPacket = audioStream.getAudioCodecConfigPacket(packet.getAbsTimecode());
										if (configPacket != null) {
											// set a timecode that doesn't interfere with others
											configPacket.setAbsTimecode(packet.getAbsTimecode());
											// update the packet's timecode
											configPacket.setAbsTimecode(configPacket.getAbsTimecode() + audioTCOffset);
											packets.add(configPacket);
										}
										// send metadata
										IMediaStreamMetaDataProvider mdProvider = audioStream.getMetaDataProvider();
										List<AMFPacket> metaData = new ArrayList<AMFPacket>();
										mdProvider.onStreamStart(metaData, packet.getAbsTimecode());
										metaData.forEach(mdPacket -> {
											// if the packet exists and has data
											if (mdPacket!=null && mdPacket.getSize()>0 && mdPacket.getData()!=null) {
												AMFPacket newPacket = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, mdPacket.getData());
												// set a timecode that doesn't interfere with others
												newPacket.setAbsTimecode(packet.getAbsTimecode() + 1);
												// update the packet's timecode
												newPacket.setAbsTimecode(newPacket.getAbsTimecode() + audioTCOffset);
												packets.add(newPacket);
											}
										});
										// update the packet's timecode, so it doesn't interfere with the others
										packet.setAbsTimecode(packet.getAbsTimecode() + audioTCOffset + 2);
										// add the current packet
										packets.add(packet);
									}
								}
							}
						}
					}
				}
				
				// if we have the same source for video and audio
				if (videoSource!=null && audioSource!=null && videoSource.getName().equals(audioSource.getName())) {
					// get the packets to play (since it's the same stream for audio and video, we just loop once throught the video stream)
					List<AMFPacket> inputPackets = videoSource.getPlayPackets();
					if ( inputPackets==null || inputPackets.size()==0 ) {
						logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": No packets to play");
					} else {
						// calculate current loop span
						int startIdx = (int) (Long.max(audioSeq, videoSeq) - inputPackets.get(0).getSeq() + 1);
						if (startIdx < 0) startIdx = 0;
						// temp variables for max timecodes and sequences for this iteration
						long maxAudioTC = -1, maxVideoTC = -1, maxAudioSeq = -1, maxVideoSeq = -1;
						// add the most recent packets to the output list
						for (int i = startIdx; i < inputPackets.size() ; i++) {
							AMFPacket packet = inputPackets.get(i).clone(true);
							// update the packet's timecode
							packet.setAbsTimecode(packet.getAbsTimecode() + audioTCOffset);
							switch (packet.getType()) {
								case IVHost.CONTENTTYPE_AUDIO:
									// add the packet if it wasn't already added last time
									if ( packet.getSeq()>audioSeq || packet.getAbsTimecode()>audioTC ) {
										// update the maximums
										if (packet.getAbsTimecode() > maxAudioTC) maxAudioTC = packet.getAbsTimecode();
										if (packet.getSeq() > maxAudioSeq) maxAudioSeq = packet.getSeq();
										// add the packet to the playback queue
										packets.add(packet);
									}
									break;
								case IVHost.CONTENTTYPE_VIDEO:
									// add the packet if it wasn't already added last time
									if ( packet.getSeq()>videoSeq || packet.getAbsTimecode()>videoTC ) {
										// update the maximums
										if (packet.getAbsTimecode() > maxVideoTC) maxVideoTC = packet.getAbsTimecode();
										if (packet.getSeq() > maxVideoSeq) maxVideoSeq = packet.getSeq();
										// add the packet to the playback queue
										packets.add(packet);
									}
									break;
								default:
									// ignore unknown packets
							}
						}
						// store max sequences and timecodes
						audioTC = Long.max(audioTC,maxAudioTC);
						videoTC = Long.max(videoTC,maxVideoTC);
						audioSeq = Long.max(audioSeq,maxAudioSeq);
						videoSeq = Long.max(videoSeq,maxVideoSeq);
					}
				} else {
					// if we have what to play on video
					if (videoSource != null) {
						// get the packets to play
						List<AMFPacket> inputPackets = videoSource.getPlayPackets();
						if ( inputPackets==null || inputPackets.size()==0 ) {
							logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": No packets to play on video");
						} else {
							// calculate current loop span
							int startIdx = (int) (videoSeq - inputPackets.get(0).getSeq() + 1);
							if (startIdx < 0) startIdx = 0;
							// temp variables for max timecodes and sequences for this iteration
							long maxVideoTC = -1, maxVideoSeq = -1;
							// add the most recent packets to the output list
							for (int i = startIdx; i < inputPackets.size() ; i++) {
								AMFPacket packet = inputPackets.get(i).clone(true);
								// first update the packet's timecode
								packet.setAbsTimecode(packet.getAbsTimecode() + videoTCOffset);
								switch (packet.getType()) {
									case IVHost.CONTENTTYPE_VIDEO:
										// add the packet if it wasn't already added last time
										if ( packet.getSeq()>videoSeq || packet.getAbsTimecode()>videoTC ) {
											// update the maximums
											if (packet.getAbsTimecode() > maxVideoTC) maxVideoTC = packet.getAbsTimecode();
											if (packet.getSeq() > maxVideoSeq) maxVideoSeq = packet.getSeq();
											// add the packet
											packets.add(packet);
											
										}
										break;
									default:
										// ignore other types of packets
								}
							}
							// store max sequences and timecodes
							videoTC = Long.max(videoTC,maxVideoTC);
							videoSeq = Long.max(videoSeq,maxVideoSeq);
						}
					}
					
					// if we have what to play on audio
					if (audioSource != null) {
						// get the packets to play
						List<AMFPacket> inputPackets = audioSource.getPlayPackets();
						if ( inputPackets==null || inputPackets.size()==0 ) {
							logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": No packets to play on audio");
						} else {
							// calculate current loop span
							int startIdx = (int) (audioSeq - inputPackets.get(0).getSeq() + 1);
							if (startIdx < 0) startIdx = 0;
							// temp variables for max timecodes and sequences for this iteration
							long maxAudioTC = -1, maxAudioSeq = -1;
							// add the most recent packets to the output list
							for (int i = startIdx; i < inputPackets.size() ; i++) {
								AMFPacket packet = inputPackets.get(i).clone(true);
								// first update the packet's timecode
								packet.setAbsTimecode(packet.getAbsTimecode() + audioTCOffset);
								switch (packet.getType()) {
									case IVHost.CONTENTTYPE_AUDIO:
										// add the packet if it wasn't already added last time
										if ( packet.getSeq()>audioSeq || packet.getAbsTimecode()>audioTC ) {
											// update the maximums
											if (packet.getAbsTimecode() > maxAudioTC) maxAudioTC = packet.getAbsTimecode();
											if (packet.getSeq() > maxAudioSeq) maxAudioSeq = packet.getSeq();
											// add the packet to the playback queue
											packets.add(packet);
										}
										break;
									default:
										// ignore other types of packets
								}
							}
							// store max sequences and timecodes
							audioTC = Long.max(audioTC,maxAudioTC);
							audioSeq = Long.max(audioSeq,maxAudioSeq);
						}
					}
				}

				// logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Current timecodes: { audio: " + audioTC + ", video: " + videoTC + " }");
				
				// create publisher if it doesn't exist, but we have packets
				if (packets.size()>0 && publisher==null) {
					if (!initPublisher()) {
						// shut down due to lack of publisher
						logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Shutting down outputstream for lack of publisher");
						shutDown();
						break;
					}
				}
				
				if (publisher != null) {
					// as long as there is a packet
					while (packets.peek() != null) {
						// get the next packet
						AMFPacket packet = packets.poll();
						// logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Writing packet " + getPacketTypeString(packet.getType()) + " " + packet);
						// output the packet
						switch (packet.getType()) {
							case IVHost.CONTENTTYPE_AUDIO:
								publisher.addAudioData(packet.getData(), packet.getSize(), packet.getAbsTimecode());
								// look for discontinuities
								if (packet.getAbsTimecode() > maxSentAudioTC) maxSentAudioTC = packet.getAbsTimecode();
								else logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Timecode discontinuity (audio) " + packet.getAbsTimecode() + " vs " + maxSentAudioTC);
								break;
							case IVHost.CONTENTTYPE_VIDEO:
								publisher.addVideoData(packet.getData(), packet.getSize(), packet.getAbsTimecode());
								// look for discontinuities
								if (packet.getAbsTimecode() > maxSentVideoTC) maxSentVideoTC = packet.getAbsTimecode();
								else logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Timecode discontinuity (video) " + packet.getAbsTimecode() + " vs " + maxSentVideoTC);
								break;
							default:
								// just add them as data packets
								publisher.addDataData(packet.getData(), packet.getSize(), packet.getAbsTimecode());
						}
					}
				}
				
				// shut down if nothing to do
				if (packets.isEmpty() && videoSource == null && audioSource == null && switchTC == Long.MAX_VALUE) {
					// shut it down after a while of doing nothing
					if (idleTime > idleShutdown) {
						logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Shutting down outputstream for lack of work");
						shutDown();
						break;
					}
					// increment the idle time (ignoring any actual execution time which is negligible)
					idleTime += sleepTime;
				} else {
					idleTime = 0;
				}
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Outputstream interrupted");
				}
			}
		} catch (Exception e) {
			logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Error for output stream " + e.getMessage() + "\nStacktrace:\n" + getStackString(e));
		} finally {
			shutDown();
		}
	}
	
	private boolean initPublisher()
	{
		// checking if stream already exists
		IMediaStream stream = appInstance.getStreams().getStream(outputName);
		if (stream != null)
		{
			logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Cannot create Publisher. Stream already exists");
			return false;
		}
		// creating publisher
		publisher = Publisher.createInstance(appInstance);
		if (publisher != null) {
			// publish stream
			publisher.setStreamType(appInstance.getStreamType());
			publisher.publish(outputName);
			// grabbing the sleep time from the stream config flush interval
			stream = publisher.getStream();
			// keep metadata up to date
			stream.setMergeOnMetadata(true);
			// grabbing the sleep time from the stream config flush interval
			sleepTime = stream.getProperties().getPropertyInt("flushInterval", sleepTime);
			logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Publisher created. Thread sleep time set to " + sleepTime);
			return true;
		}
		logger.error(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Cannot create Publisher");
		return false;
	}
	
	private void sendConfirmation(String targetStream, String videoName, String audioName) {
		
		if (targetStream == null) return;
		
		boolean overlayActive = false;
		
		if (StreamCreatorConsumer.getInstance().getOverlayedStreams().containsValue(targetStream)) {
			targetStream = targetStream.replace("-overlay","");
			overlayActive = true;
		}
		
		String actualAudioName = audioName;
		String actualVideoName = videoName;
		
		Stream actualStream = StreamCreatorConsumer.getInstance().getAudioStreamList().get(appInstance.getContextStr() + "/" + targetStream);
		if (actualStream != null) {
			PlaylistItem item = actualStream.getCurrentItem();
			if (item != null)
				actualAudioName = actualStream.getCurrentItem().getName();
		}
		
		actualStream = StreamCreatorConsumer.getInstance().getVideoStreamList().get(appInstance.getContextStr() + "/" + targetStream);
		if (actualStream != null) {
			PlaylistItem item = actualStream.getCurrentItem();
			if (item != null)
				actualVideoName = actualStream.getCurrentItem().getName();
		}
		
		String routingKey = routingKeyTemplate;
		try {
			String[] pieces = targetStream.split("_");
			Integer presentationId = Integer.decode(pieces[pieces.length -1]);
			routingKey = MessageFormat.format(routingKeyTemplate, presentationId);
		} catch (Exception e) {
			logger.error(CLASS_NAME+".sendConfirmation: cannot get presentation id from stream " + targetStream + ". Error: " + e.getMessage());
		}
		
		Channel channel = RabbitSingleton.getInstance().getRabbitChannel();
		
		if (channel != null && channel.isOpen()) {
			StreamConfirmData data = new StreamConfirmData();
			data.setAppName(appInstance.getContextStr());
			data.setStreamName(targetStream);
			data.setVideoName(actualVideoName);
			data.setAudioName(actualAudioName);
			data.setvHostName(vHostName);
			data.setServerName(serverName);
			data.setOverlayActive(overlayActive);
			StreamConfirmMessage message = new StreamConfirmMessage();
			message.setData(data);
			message.setTimestamp(Instant.now().getEpochSecond());
			message.setType(MessageType.SWITCH_CONFIRM);
			try {
				channel.basicPublish(exchangeName, routingKey, null, json.writeValueAsBytes(message));
				logger.info(CLASS_NAME+".sent confirmation message: " + json.writeValueAsString(message) + " on exchange name " + exchangeName + " and routing key " + routingKey);
			} catch (Exception e) {
				logger.error(CLASS_NAME+".sendConfirmation: " + e.getMessage());
			}
		} else {
			logger.error(CLASS_NAME+".sendConfirmation: channel to rabbit is unexplicably closed");
		}
	}
	
	private void shutDown() {
		// clean up the publisher
		if (publisher != null) {
			publisher.unpublish();
			publisher.close();
		}
		publisher = null;
		running = false;
	}
	
	public String getAudioName() {
		return audioName;
	}

	public void setAudioName(String audioName) {
		if (audioName != null)
			synchronized (lock) {
				this.audioName = audioName;
				logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] " + ": Audio name updated to " + audioName);
			}
	}

	public String getVideoName() {
		return videoName;
	}

	public void setVideoName(String videoName) {
		if (videoName != null)
			synchronized (lock) {
				this.videoName = videoName;
				nxtVideoSource = null;
				switchTC = Long.MAX_VALUE;
				logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] "+ ": Video name updated to " + videoName);
			}
	}
	
	public void setAudioVideoName(String name) {
		if (name != null)
			synchronized (lock) {
				this.audioName = name;
				this.videoName = name;
				nxtVideoSource = null;
				switchTC = Long.MAX_VALUE;
				logger.info(CLASS_NAME + this.hashCode() + " [ " + appInstance.getContextStr() + "/" + outputName + " ] "+ ": Video and audio names updated to " + name);
			}
	}

	public String getOutputName() {
		return outputName;
	}

	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}

	public void close() {
		synchronized(lock) {
			doQuit = true;
			interrupt();
		}
	}

	public boolean isRunning() {
		synchronized(lock) {
			return running;
		}
	}
	
	private String getStackString(Exception e) {
		StringWriter writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		e.printStackTrace(printWriter);
		printWriter.flush();
		return writer.toString();
	}
	
	private String getPacketTypeString(int type) {
		switch (type) {
			case IVHost.CONTENTTYPE_VIDEO:
				return "video";
			case IVHost.CONTENTTYPE_AUDIO:
				return "audio";
			case IVHost.CONTENTTYPE_DATA:
			case IVHost.CONTENTTYPE_DATA3:
				return "data";
			default:
				return "other";
		}
	}
}
