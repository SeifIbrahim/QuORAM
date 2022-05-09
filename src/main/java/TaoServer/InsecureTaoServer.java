package TaoServer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Messages.ClientRequest;
import Messages.MessageCreator;
import Messages.MessageTypes;
import Messages.ProxyResponse;
import TaoProxy.Block;
import TaoProxy.MessageUtility;
import TaoProxy.Tag;
import TaoProxy.TaoBlock;
import TaoProxy.TaoBucket;
import TaoProxy.TaoLogger;
import TaoProxy.TaoMessageCreator;

public class InsecureTaoServer extends TaoServer {

	public InsecureTaoServer(MessageCreator messageCreator, int unitId) {
		super(messageCreator, unitId);
		TaoLogger.logLevel = TaoLogger.LOG_OFF;
	}

	/**
	 * @brief This is changed to serve a client instead of a proxy, by serving unencrypted blocks instead of paths
	 * @param channel
	 */
	protected void serveProxy(AsynchronousSocketChannel channel) {
		try {
			// Create a ByteBuffer to read in message type
			ByteBuffer typeByteBuffer = MessageUtility.createTypeReceiveBuffer();

			// Read the initial header
			Future<Integer> initRead = channel.read(typeByteBuffer);
			initRead.get();

			// Flip the byte buffer for reading
			typeByteBuffer.flip();

			TaoLogger.logInfo("Server received a client request");

			// Figure out the type of the message
			int[] typeAndLength;
			try {
				typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
			} catch (BufferUnderflowException e) {
				TaoLogger.logForce("Lost connection to client");
				try {
					channel.close();
				} catch (IOException e1) {
					e1.printStackTrace(System.out);
				}
				return;
			}
			int messageType = typeAndLength[0];
			int messageLength = typeAndLength[1];

			// Read rest of message
			ByteBuffer message = ByteBuffer.allocate(messageLength);
			while (message.remaining() > 0) {
				Future<Integer> entireRead = channel.read(message);
				entireRead.get();
			}

			// Flip buffer for reading
			message.flip();

			// Get bytes from message
			byte[] requestBytes = new byte[messageLength];
			message.get(requestBytes);

			// Clear buffer
			message = null;

			// Create ClientRequest object based on read bytes
			ClientRequest clientReq = mMessageCreator.createClientRequest();
			clientReq.initFromSerialized(requestBytes);
			clientReq.setChannel(channel);

			// Serve message based on type
			if (messageType == MessageTypes.CLIENT_READ_REQUEST) {
				mReadStartTimes.put(clientReq.hashCode(), System.currentTimeMillis());
				mReadPathExecutor.submit(() -> {
					TaoLogger.logDebug("Serving a read request");
					// Read the requested block
					TaoBlock block = (TaoBlock) readBlock(clientReq.getBlockID());

					long startTime = mReadStartTimes.remove(clientReq.hashCode());
					long time = System.currentTimeMillis() - startTime;

					// Create a server response
					ProxyResponse readResponse = mMessageCreator.createProxyResponse();
					readResponse.setProcessingTime(time);
					readResponse.setClientRequestID(clientReq.getRequestID());
					readResponse.setReturnData(block.getData());
					readResponse.setReturnTag(block.getTag());

					// To be used for server response
					byte[] messageTypeAndLength = null;
					byte[] serializedResponse = null;

					// Create server response data and header
					serializedResponse = readResponse.serialize();
					messageTypeAndLength = Bytes.concat(Ints.toByteArray(MessageTypes.PROXY_RESPONSE),
							Ints.toByteArray(serializedResponse.length));

					// Create message to send to proxy
					ByteBuffer returnMessageBuffer = ByteBuffer
							.wrap(Bytes.concat(messageTypeAndLength, serializedResponse));

					try {
						// Write to proxy
						TaoLogger.logDebug("Going to send response of size " + serializedResponse.length);
						while (returnMessageBuffer.remaining() > 0) {
							Future writeToProxy = channel.write(returnMessageBuffer);
							writeToProxy.get();
						}
						TaoLogger.logDebug("Sent response");

						// Clear buffer
						returnMessageBuffer = null;
					} catch (Exception e) {
						try {
							channel.close();
						} catch (IOException e1) {
						}
					} finally {
						// Serve the next proxy request
						Runnable serializeProcedure = () -> serveProxy(channel);
						new Thread(serializeProcedure).start();
					}
				});
			} else if (messageType == MessageTypes.CLIENT_WRITE_REQUEST) {
				mWriteStartTimes.put(clientReq.hashCode(), System.currentTimeMillis());
				mWriteBackExecutor.submit(() -> {
					TaoLogger.logDebug("Serving a write request");
					// Write the requested block
					writeBlock(clientReq.getBlockID(), clientReq.getData(), clientReq.getTag());

					long startTime = mWriteStartTimes.remove(clientReq.hashCode());
					long time = System.currentTimeMillis() - startTime;

					ProxyResponse writeResponse = mMessageCreator.createProxyResponse();
					writeResponse = mMessageCreator.createProxyResponse();
					writeResponse.setProcessingTime(time);
					writeResponse.setClientRequestID(clientReq.getRequestID());
					writeResponse.setWriteStatus(true);
					writeResponse.setReturnTag(clientReq.getTag());

					// To be used for server response
					byte[] messageTypeAndLength = null;
					byte[] serializedResponse = null;

					// Create server response data and header
					serializedResponse = writeResponse.serialize();
					messageTypeAndLength = Bytes.concat(Ints.toByteArray(MessageTypes.PROXY_RESPONSE),
							Ints.toByteArray(serializedResponse.length));

					// Create message to send to proxy
					ByteBuffer returnMessageBuffer = ByteBuffer
							.wrap(Bytes.concat(messageTypeAndLength, serializedResponse));

					try {
						// Write to proxy
						TaoLogger.logDebug("Going to send response of size " + serializedResponse.length);
						while (returnMessageBuffer.remaining() > 0) {
							Future writeToProxy = channel.write(returnMessageBuffer);
							writeToProxy.get();
						}
						TaoLogger.logDebug("Sent response");

						// Clear buffer
						returnMessageBuffer = null;
					} catch (Exception e) {
						try {
							channel.close();
						} catch (IOException e1) {
						}
					} finally {
						// Serve the next proxy request
						Runnable serializeProcedure = () -> serveProxy(channel);
						new Thread(serializeProcedure).start();
					}
				});
			} else {
				TaoLogger.logForce("Unknown MessageType: " + messageType);
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace(System.out);
		}
	}

	private void writeBlock(long blockID, byte[] data, Tag tag) {
		try {
			// Grab a file pointer from shared pool
			/*
			while (true) {
				try {
					mFilePointersSemaphore.acquire();
					break;
				} catch (InterruptedException e) {
				}
			}
			*/
			RandomAccessFile diskFile = null;
			synchronized (mFilePointers) {
				diskFile = mFilePointers.pop();
			}

			long bucketIndex = blockID / TaoConfigs.BLOCKS_IN_BUCKET;
			// Lock appropriate bucket
			mBucketLocks[(int) bucketIndex].lock();

			// Seek into the file
			diskFile.seek(TaoConfigs.BUCKET_SIZE * bucketIndex);

			// Read the bucket for the block
			byte[] bucketData = new byte[TaoConfigs.BUCKET_SIZE];
			diskFile.readFully(bucketData);
			TaoBucket bucket = new TaoBucket();
			bucket.initFromSerialized(bucketData);

			// update the block
			Block block = bucket.getBlocks()[(int) (blockID % TaoConfigs.BLOCKS_IN_BUCKET)];
			block.setBlockID(blockID);
			block.setData(data);
			block.setTag(tag);

			// Seek again to write
			diskFile.seek(TaoConfigs.BUCKET_SIZE * bucketIndex);

			diskFile.write(bucket.serialize());

			mBucketLocks[(int) bucketIndex].unlock();
			// Release the file pointer lock
			synchronized (mFilePointers) {
				mFilePointers.push(diskFile);
			}
			// mFilePointersSemaphore.release();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	private Block readBlock(long blockID) {
		try {
			// Grab a file pointer from shared pool
			/*
			while (true) {
				try {
					mFilePointersSemaphore.acquire();
					break;
				} catch (InterruptedException e) {
				}
			}
			*/
			RandomAccessFile diskFile = null;
			synchronized (mFilePointers) {
				diskFile = mFilePointers.pop();
			}

			long bucketIndex = blockID / TaoConfigs.BLOCKS_IN_BUCKET;
			// Lock appropriate bucket
			mBucketLocks[(int) bucketIndex].lock();

			// Seek into the file
			diskFile.seek(TaoConfigs.BUCKET_SIZE * bucketIndex);

			// Read bytes from the disk file into the byte array for this bucket
			byte[] bucketData = new byte[TaoConfigs.BUCKET_SIZE];
			diskFile.readFully(bucketData);

			mBucketLocks[(int) bucketIndex].unlock();
			// Release the file pointer lock
			synchronized (mFilePointers) {
				mFilePointers.push(diskFile);
			}
			// mFilePointersSemaphore.release();

			TaoBucket bucket = new TaoBucket();
			bucket.initFromSerialized(bucketData);
			return bucket.getBlocks()[(int) (blockID % TaoConfigs.BLOCKS_IN_BUCKET)];
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		return null;
	}

	protected void initializeStorage() {
		try {
			long numBuckets = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
			RandomAccessFile diskFile = mFilePointers.pop();
			TaoBucket emptyBucket = new TaoBucket();
			byte[] emptyBucketBytes = emptyBucket.serialize();
			for (long i = 0; i < numBuckets; ++i) {
				diskFile.write(emptyBucketBytes);
			}
			mFilePointers.push(diskFile);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static void main(String[] args) {
		try {
			// Parse any passed in args
			Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

			// Determine if the user has their own configuration file name, or just use the
			// default
			String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
			TaoConfigs.USER_CONFIG_FILE = configFileName;

			int unitId = Integer.parseInt(options.get("unit"));

			// Create server and run
			Server server = new InsecureTaoServer(new TaoMessageCreator(), unitId);
			((InsecureTaoServer) server).initializeStorage();
			server.run();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}
}
