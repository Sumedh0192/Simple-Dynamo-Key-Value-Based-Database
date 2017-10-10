package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static Map<String, String> databaseMap;
	private static Map<String, Map<String, String>> predeccessorDatabaseMap;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int JOIN_TIMEOUT = 5000;
	static final int SERVER_PORT = 10000;
	private final Uri pa3Uri = buildUri("content", "edu.buffalo.cse.cse486586.SimpleDynamo.provider");

	static String node_id;
	static String myPort;
	static Integer chordRingIndex;
	static List<String> chordRing = new ArrayList<String>(5);
	static Map<String, String> chordsKeyMap = new HashMap<String, String>();
	static Map<String, String> keyVersionMap = new HashMap<String, String>();
	static Map<String, Integer> keyVersioningMap = new HashMap<String, Integer>();
	static Boolean isRecoveryOn = false;


	private static final String[] Nodes = {"5554", "5556", "5558", "5560", "5562"};


	private Uri buildUri(String scheme, String authority) {

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public String getCurrentTimeStamp() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
	}

	public Boolean isDateGreater(String firstDate, String SecondDate){
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			return sdf.parse(firstDate).before(sdf.parse(SecondDate));
		}catch (Exception ex){
			ex.printStackTrace();
		}
		return null;
	}

	private class MessageReceiver extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket listner = null;
			ObjectOutputStream sendObjectStream;
			while (true) {
				try {
					listner = serverSocket.accept();
					ObjectInputStream receivedObjectStream = new ObjectInputStream(listner.getInputStream());
					Object receivedObject = receivedObjectStream.readObject();
					OperationClass operationClassObject = null;
					RecoveryClass recoveryClassObject = null;
					if (receivedObject instanceof OperationClass) {
						operationClassObject = (OperationClass) receivedObject;
					}
					if (receivedObject instanceof RecoveryClass) {
						recoveryClassObject = (RecoveryClass) receivedObject;
					}
					if (recoveryClassObject != null) {
						System.out.println("###Recovery## Request Received #### ");
						if(recoveryClassObject.isRecoveryRequest != null || recoveryClassObject.isReplicaRecovery != null) {
							if (recoveryClassObject.isRecoveryRequest) {
								System.out.println("###Recovery## Request Recovery #### From  " + recoveryClassObject.portnum + "  To  " + myPort);
								sendObjectStream = new ObjectOutputStream(listner.getOutputStream());
								if (recoveryClassObject.isFirstSuccessor) {
									if (predeccessorDatabaseMap.containsKey(recoveryClassObject.firstPredeccessor)) {
										recoveryClassObject.backupDataMap = predeccessorDatabaseMap.get(recoveryClassObject.firstPredeccessor);
										System.out.println("###Recovery##  First Pred Map Size:  " + predeccessorDatabaseMap.get(recoveryClassObject.firstPredeccessor).size());
									}
								}
								if( predeccessorDatabaseMap.get(recoveryClassObject.portnum) != null)
									System.out.println("###Recovery##  Recovery Map Size:  " + predeccessorDatabaseMap.get(recoveryClassObject.portnum).size());
								else
									System.out.println("###Recovery##  Recovery Map Size: " + recoveryClassObject.portnum +  " null");
								recoveryClassObject.recoveryDataMap = predeccessorDatabaseMap.get(recoveryClassObject.portnum);
								recoveryClassObject.versioningMap = keyVersioningMap;
								recoveryClassObject.portnum = myPort;
								sendObjectStream.writeObject(recoveryClassObject);
								sendObjectStream.flush();
								//sendObjectStream.close();
							} else if (recoveryClassObject.isReplicaRecovery) {
								System.out.println("###Recovery## Request Replica #### From  " + recoveryClassObject.portnum + "  To  " + myPort);
								sendObjectStream = new ObjectOutputStream(listner.getOutputStream());
								if (recoveryClassObject.isFirstPredeccessor) {
									if (predeccessorDatabaseMap.containsKey(recoveryClassObject.secondPredeccessor)) {
										recoveryClassObject.backupDataMap = predeccessorDatabaseMap.get(recoveryClassObject.secondPredeccessor);
										System.out.println("###Recovery##  Second Pred Backup Map Size:  " + predeccessorDatabaseMap.get(recoveryClassObject.secondPredeccessor).size());
									}
								}
								if( databaseMap != null)
									System.out.println("###Recovery##  Replica Map Size:  " + databaseMap.size() + " Port "  + myPort);
								else
									System.out.println("###Recovery##  Replica Map Size:  null");
								recoveryClassObject.recoveryDataMap = databaseMap;
								recoveryClassObject.versioningMap = keyVersioningMap;
								recoveryClassObject.portnum = myPort;
								sendObjectStream.writeObject(recoveryClassObject);
								sendObjectStream.flush();
								//sendObjectStream.close();
							}
						}
					}
					if (operationClassObject != null) {
						if(operationClassObject.type != null) {
							if (operationClassObject.type.compareTo("insert") == 0) {
								System.out.println(" #### Server Insert  #### ");
								ContentValues values = new ContentValues();
								values.put("key", operationClassObject.key);
								values.put("value", operationClassObject.value);
								insert(values, operationClassObject.parentPort);
							} else if (operationClassObject.type.compareTo("delete") == 0) {
								if (operationClassObject.selection.compareTo("\"@\"") == 0) {
									delete(pa3Uri, operationClassObject.selection, null);
								} else {
									delete(operationClassObject.selection, operationClassObject.parentPort);
								}
							} else if (operationClassObject.type.compareTo("query") == 0) {
								Cursor cursor = query(operationClassObject.selection);
								Map<String, String> returnMap = new HashMap<String, String>();
								if (cursor != null) {
									while (cursor.moveToNext()) {
										returnMap.put(cursor.getString(cursor.getColumnIndexOrThrow("key")), cursor.getString(cursor.getColumnIndexOrThrow("value")));
									}
								}
								OperationResponseClass optRes = new OperationResponseClass(null, returnMap);
								if (operationClassObject.selection.length() > 5) {
									if (keyVersioningMap.containsKey(operationClassObject.selection)) {
										optRes.version = keyVersioningMap.get(operationClassObject.selection);
									}else{
										optRes.version = 0;
									}
									System.out.println("#### Return Single Query " + operationClassObject.selection + " --> " + returnMap.get(operationClassObject.selection));
								}
								sendObjectStream = new ObjectOutputStream(listner.getOutputStream());
								sendObjectStream.writeObject(optRes);
								sendObjectStream.flush();
							}
						}
					}
					System.out.println("######### Outside of If ###### ");
				} catch (Exception e) {
					Log.e(TAG, "Error in Receiving the message");
					Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
					e.printStackTrace();
				} finally {
					try {
						listner.close();
					} catch (IOException ex) {
						Log.e(TAG, "Exception-->  " + ex.getMessage());
						Log.e(TAG, "Exception--->  " + ex.getStackTrace());
						ex.printStackTrace();
					}
				}
			}
		}
		protected void onProgressUpdate(String... msgs) {
			return;
		}
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		while(true)if(!isRecoveryOn)break;
		try {
			int rowsDeleted = 0;
			if (selection.compareTo("\"*\"") == 0) {
				Socket socket = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;
				Object receivedObject;
				OperationResponseClass optResponse;
				for (String remotePort : chordsKeyMap.values()) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));
						out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						out.writeObject(new OperationClass("delete", "\"@\"", null, null, null));
						out.flush();
						while (!socket.isClosed()) {
							in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
							receivedObject = in.readObject();
							if (receivedObject != null) {
								if (receivedObject instanceof OperationResponseClass) {
									optResponse = (OperationResponseClass) receivedObject;
									if (optResponse.deletedRows != null) {
										rowsDeleted += optResponse.deletedRows;
										break;
									}
								}
							}
						}
					} catch (UnknownHostException e) {
						Log.e(TAG, "SendTask UnknownHostException");
						Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					} catch (IOException e) {
						Log.e(TAG, "SendTask socket IOException");
						Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					} finally {
						try {
							out.close();
							socket.close();
						} catch (IOException ex) {
							Log.d(TAG, "Exception-->  " + ex.getMessage());
							Log.d(TAG, "Exception--->  " + ex.getStackTrace());
						}
					}
				}
			} else if (selection.compareTo("\"@\"") == 0) {
				rowsDeleted = databaseMap.size();
				for (String predeccessorPort : predeccessorDatabaseMap.keySet()) {
					rowsDeleted += predeccessorDatabaseMap.get(predeccessorPort).size();
				}
				databaseMap = new HashMap<String, String>();
				predeccessorDatabaseMap = new HashMap<String, Map<String, String>>();

			} else {
				String keyHash = genHash(selection);
				final String nodeToDelete = getOperationNode(keyHash);
				List<String> portSet = getSuccessors(chordsKeyMap.get(nodeToDelete));
				portSet.add(chordsKeyMap.get(nodeToDelete));
				Socket socket = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;
				Object receivedObject;
				OperationResponseClass optResponse;
				for (String remotePort : portSet) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));
						out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						out.writeObject(new OperationClass("delete", selection, null, null, chordsKeyMap.get(nodeToDelete)));
						out.flush();
						while (!socket.isClosed()) {
							in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
							receivedObject = in.readObject();
							if (receivedObject != null) {
								if (receivedObject instanceof OperationResponseClass) {
									optResponse = (OperationResponseClass) receivedObject;
									if (optResponse.deletedRows != null) {
										rowsDeleted += optResponse.deletedRows;
										break;
									}
								}
							}
						}
					} catch (UnknownHostException e) {
						Log.e(TAG, "SendTask UnknownHostException");
						Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					} catch (IOException e) {
						Log.e(TAG, "SendTask socket IOException");
						Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					} finally {
						try {
							out.close();
							socket.close();
						} catch (IOException ex) {
							Log.d(TAG, "Exception-->  " + ex.getMessage());
							Log.d(TAG, "Exception--->  " + ex.getStackTrace());
						}
					}
				}
				rowsDeleted = 1;
			}
			return rowsDeleted;
		} catch (Exception ex) {
			Log.e(TAG, " Exception --->  " + ex.getMessage());
			ex.printStackTrace();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		while(true)if(!isRecoveryOn)break;
		try {
			String keyHash = genHash(values.get("key").toString());
			final String nodeToInsert = getOperationNode(keyHash);
			List<String> portSet = getSuccessors(chordsKeyMap.get(nodeToInsert));
			System.out.println("@@@@@@@@@@@@@@@ Key -- " + values.get("key").toString() + "   " + portSet);
			portSet.add(chordsKeyMap.get(nodeToInsert));
			Socket socket = null;
			ObjectOutputStream out = null;
			for (String remotePort : portSet) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					out.writeObject(new OperationClass("insert", null, values.get("key").toString(), values.get("value").toString(), chordsKeyMap.get(nodeToInsert)));
					out.flush();
				} catch (UnknownHostException e) {
					Log.e(TAG, "SendTask UnknownHostException");
					Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} catch (IOException e) {
					Log.e(TAG, "SendTask socket IOException");
					Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} finally {
					try {
						out.close();
						socket.close();
					} catch (IOException ex) {
						Log.d(TAG, "Exception-->  " + ex.getMessage());
						Log.d(TAG, "Exception--->  " + ex.getStackTrace());
						ex.printStackTrace();
					}
				}
			}
		} catch (Exception ex) {
			Log.e(TAG, " Exception ---> Insert " + ex.getMessage());
			ex.printStackTrace();
		}
		// TODO Auto-generated method stub
		return null;
	}

	public void insert(ContentValues values, String portNum) {
		try {
			Map<String, String> predessorDB;
			System.out.println("");
			if (portNum.compareTo(myPort) == 0) {
				databaseMap.put(values.get("key").toString(), values.get("value").toString());
			} else {
				if (predeccessorDatabaseMap.containsKey(portNum)) {
					if(predeccessorDatabaseMap.get(portNum) == null){
						predeccessorDatabaseMap.put(portNum, new HashMap<String, String>());
					}
					predessorDB = predeccessorDatabaseMap.get(portNum);
					predessorDB.put(values.get("key").toString(), values.get("value").toString());
					predeccessorDatabaseMap.put(portNum, predessorDB);
				} else {
					predessorDB = new HashMap<String, String>();
					predessorDB.put(values.get("key").toString(), values.get("value").toString());
					predeccessorDatabaseMap.put(portNum, predessorDB);
				}
			}
			keyVersionMap.put(values.get("key").toString(), getCurrentTimeStamp());
			if(keyVersioningMap.containsKey(values.get("key").toString())) {
				keyVersioningMap.put(values.get("key").toString(), keyVersioningMap.get(values.get("key").toString()) + 1);
			} else {
				keyVersioningMap.put(values.get("key").toString(), 1);
			}
		}catch (Exception ex){
			ex.printStackTrace();
		}
		System.out.println("@@@@@Insert@@@@@ Done " + values.get("key").toString() + " --> "+  values.get("value").toString() + "  v->  " + keyVersioningMap.get(values.get("key").toString()));
	}

	public Integer delete(String key, String portNum) {
		try {
			System.out.println("Replication Delete " + key + "  " + portNum);
			Map<String, String> predessorDB;
			if (portNum.compareTo(myPort) == 0) {
				if (databaseMap.containsKey(key)) {
					databaseMap.remove(key);
					return 1;
				}
			} else {
				if (predeccessorDatabaseMap.containsKey(portNum)) {
					predessorDB = predeccessorDatabaseMap.get(portNum);
					if (predessorDB.containsKey(key)) {
						predessorDB.remove(key);
						predeccessorDatabaseMap.put(portNum, predessorDB);
						return 1;
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return 0;
	}


	public List<String> getPredecessors(final String port) {
		List<String> returnList = new ArrayList<String>(2);
		if(port.compareTo("11108") == 0){
			returnList.add("11112");
			returnList.add("11124");
		}else if(port.compareTo("11112") == 0){
			returnList.add("11124");
			returnList.add("11120");
		}else if(port.compareTo("11116") == 0){
			returnList.add("11108");
			returnList.add("11112");
		}else if(port.compareTo("11120") == 0){
			returnList.add("11116");
			returnList.add("11108");
		}else if(port.compareTo("11124") == 0){
			returnList.add("11120");
			returnList.add("11116");
		}
		return returnList;
	}

	public List<String> getSuccessors(final String port) {
		List<String> returnList = new ArrayList<String>(2);
		if(port.compareTo("11108") == 0){
			returnList.add("11116");
			returnList.add("11120");
		}else if(port.compareTo("11112") == 0){
			returnList.add("11108");
			returnList.add("11116");
		}else if(port.compareTo("11116") == 0){
			returnList.add("11120");
			returnList.add("11124");
		}else if(port.compareTo("11120") == 0){
			returnList.add("11124");
			returnList.add("11112");
		}else if(port.compareTo("11124") == 0){
			returnList.add("11112");
			returnList.add("11108");
		}
		return returnList;
	}

	@Override
	public boolean onCreate() {
		try {
			Context context = getContext();
			TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

			myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			System.out.println("@@@ My Port" + myPort);
			databaseMap = new HashMap<String, String>();
			predeccessorDatabaseMap = new HashMap<String, Map<String, String>>();
			formChordsRing();
			node_id = genHash(portStr);
			File checkingFile = new File(context.getFilesDir(),myPort);
			if (checkingFile.exists()) {
				new RecoveryManager().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"");
			} else {
				checkingFile.createNewFile();
			}
			try {
				System.out.println("####### Initiate Server  ######## ");
				ServerSocket serverSocket = new ServerSocket();
				serverSocket.setReuseAddress(true);
				serverSocket.bind(new InetSocketAddress(SERVER_PORT));
				new MessageReceiver().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e(TAG, "Can't create a ServerSocket");
				Log.e(TAG, "Exception-->  " + e.getMessage());
				Log.e(TAG, "Exception--->  " + e.getStackTrace());
				e.printStackTrace();
				return false;
			}
		} catch (Exception ex) {
			Log.d(TAG, "Exception-->  " + ex.getMessage());
			ex.printStackTrace();
		}
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		while(true)if(!isRecoveryOn)break;
		System.out.println("######Query Received  " + selection);
		try {
			MatrixCursor cursorBuilder = new MatrixCursor(new String[]{"key", "value"});
			Cursor returnCursor = null;
			if (selection.compareTo("\"*\"") == 0 || selection.compareTo("*") == 0) {
				Map<String, String> responseMap = new HashMap<String, String>();
				Socket socket = null;
				ObjectOutputStream out = null;
				ObjectInputStream in = null;
				Object receivedObject;
				OperationResponseClass optResponse;
				for (String remotePort : chordsKeyMap.values()) {
					Log.d(TAG, remotePort + " #---# " + chordsKeyMap.get(node_id));
					if (remotePort.compareTo(chordsKeyMap.get(node_id)) != 0) {
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remotePort));
							out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
							Object returnObj = new OperationClass("query", "\"@\"", null, null, null);
							out.writeObject(returnObj);
							out.flush();
							while (!socket.isClosed()) {
								in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
								receivedObject = in.readObject();
								if (receivedObject != null) {
									if (receivedObject instanceof OperationResponseClass) {
										optResponse = (OperationResponseClass) receivedObject;
										if (optResponse.queriedRecords != null) {
											responseMap.putAll(optResponse.queriedRecords);
											break;
										}
									}
								}
							}
						} catch (UnknownHostException e) {
							Log.e(TAG, "SendTask UnknownHostException");
							Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
						} catch (IOException e) {
							Log.e(TAG, "SendTask socket IOException");
							Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
						} finally {
							try {
								out.close();
								socket.close();
							} catch (IOException ex) {
								Log.d(TAG, "Exception-->  " + ex.getMessage());
								Log.d(TAG, "Exception--->  " + ex.getStackTrace());
							}
						}
					} else if (remotePort.compareTo(chordsKeyMap.get(node_id)) == 0) {
						responseMap.putAll(databaseMap);
					}
				}
				if (responseMap.size() > 0) {
					for (String key : responseMap.keySet()) {
						cursorBuilder.addRow(new String[]{key, responseMap.get(key)});
					}
					Cursor[] cursors = {cursorBuilder, returnCursor};
					returnCursor = new MergeCursor(cursors);
					return returnCursor;
				}
			} else if (selection.compareTo("\"@\"") == 0 || selection.compareTo("@") == 0) {
				System.out.println("##### Query INSIDE @");
				return query(selection);
			} else {
				String keyHash = genHash(selection);
				System.out.println("###### Query  " + selection + "  " + keyHash);
				final String nodeToQuery = getOperationNode(keyHash);
				List<String> portSet = getSuccessors(chordsKeyMap.get(nodeToQuery));
				portSet.add(chordsKeyMap.get(nodeToQuery));
				Socket socket = null;
				ObjectOutputStream out = null;
				ObjectInputStream in;
				Object receivedObject;
				OperationResponseClass optResponse;
				String returnValue = "";
				Integer highestVersion = 0;
				String Port = "";
				for (String remotePort : portSet) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));
						out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						out.writeObject(new OperationClass("query", selection, null, null, null));
						out.flush();
						while (!socket.isClosed()) {
							in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
							receivedObject = in.readObject();
							if (receivedObject != null) {
								if (receivedObject instanceof OperationResponseClass) {
									optResponse = (OperationResponseClass) receivedObject;
									if (optResponse.queriedRecords != null) {
										if (optResponse.queriedRecords.containsKey(selection)) {
											if (highestVersion == 0) {
												returnValue = optResponse.queriedRecords.get(selection);
												highestVersion = optResponse.version;
												Port = remotePort;
											} else if (highestVersion <= optResponse.version) {
												returnValue = optResponse.queriedRecords.get(selection);
												highestVersion = optResponse.version;
												Port = remotePort;
											}
										}
									}
									break;
								}
							}
						}
						System.out.println(" ### Query after while #### " + selection);
					} catch (UnknownHostException e) {
						Log.e(TAG, "SendTask UnknownHostException");
						Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
						e.printStackTrace();
					} catch (IOException e) {
						Log.e(TAG, "SendTask socket IOException");
						Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
						e.printStackTrace();
					} finally {
						try {
							out.close();
							socket.close();
						} catch (IOException ex) {
							Log.d(TAG, "Exception-->  " + ex.getMessage());
							Log.d(TAG, "Exception--->  " + ex.getStackTrace());
							ex.printStackTrace();
						}
					}
				}
				System.out.println(" ##### return Value " + returnValue);
				if (returnValue.compareTo("") != 0) {
/*					if(keyVersioningMap.containsKey(selection)) {
						if (keyVersioningMap.get(selection) < highestVersion) {
							if (databaseMap.containsKey(selection)) {
								databaseMap.put(selection, returnValue);
								keyVersioningMap.put(selection, highestVersion);
							} else {
								for (String port : predeccessorDatabaseMap.keySet()) {
									if (predeccessorDatabaseMap.get(port).containsKey(selection)) {
										Map<String, String> tempMap = predeccessorDatabaseMap.get(port);
										tempMap.put(selection, returnValue);
										predeccessorDatabaseMap.put(port, tempMap);
										keyVersioningMap.put(selection, highestVersion);
									}
								}
							}
						}
					}*/
					cursorBuilder.addRow(new String[]{selection, returnValue});
					Cursor[] cursors = {cursorBuilder, returnCursor};
					returnCursor = new MergeCursor(cursors);
					System.out.println("###### Query  Output " + selection + "   " + returnValue + "  port " + Port);
					return returnCursor;
				}
			}
			return null;
		} catch (Exception ex) {
			Log.e(TAG, " Query Main Exception --->  " + ex.getMessage());
			ex.printStackTrace();
		}
		return null;
		// TODO Auto-generated method stub
	}

	public static Cursor query(String selection){
		try {
			System.out.println("#### Inside Query 2  " + selection);
			MatrixCursor cursorBuilder = new MatrixCursor(new String[]{"key", "value"});
			Cursor returnCursor = null;
			if (selection.compareTo("\"@\"") == 0 || selection.compareTo("@") == 0) {
				System.out.println("##### Query INSIDE @@@@@@@@@@@@@@@@@@@@@@@@@");
				if (databaseMap.size() > 0) {
					for (String key : databaseMap.keySet()) {
						cursorBuilder.addRow(new String[]{key, databaseMap.get(key)});
						System.out.println("@@@@@@@@@@ Port " + myPort + "  " + key + " --> " + databaseMap.get(key) + " version  " + keyVersioningMap.get(key));
					}
				}
				if (predeccessorDatabaseMap.size() > 0) {
					for (String predecessorPort : predeccessorDatabaseMap.keySet()) {
						if(predeccessorDatabaseMap.get(predecessorPort) != null) {
							for (String key : predeccessorDatabaseMap.get(predecessorPort).keySet()) {
								cursorBuilder.addRow(new String[]{key, predeccessorDatabaseMap.get(predecessorPort).get(key)});
								System.out.println("@@@@@@@@@@ Port " + myPort + "  " + key + " --> " + predeccessorDatabaseMap.get(predecessorPort).get(key) + " version  " + keyVersioningMap.get(key));
							}
						}
					}
				}
				Cursor[] cursors = {cursorBuilder, returnCursor};
				returnCursor = new MergeCursor(cursors);
				System.out.println("##### Query @ Done @@@@@@@@@@@@@@@@@@@@@@@@@");
				return returnCursor;
			} else {
				System.out.println("### Query Single in Custom Query " + selection);
				if (databaseMap.containsKey(selection)) {
					cursorBuilder.addRow(new String[]{selection, databaseMap.get(selection)});
					Cursor[] cursors = {cursorBuilder, returnCursor};
					returnCursor = new MergeCursor(cursors);
					return returnCursor;
				} else {
					for (String port : predeccessorDatabaseMap.keySet()) {
						if (predeccessorDatabaseMap.get(port).containsKey(selection)) {
							cursorBuilder.addRow(new String[]{selection, predeccessorDatabaseMap.get(port).get(selection)});
							Cursor[] cursors = {cursorBuilder, returnCursor};
							returnCursor = new MergeCursor(cursors);
							return returnCursor;
						}
					}
				}
			}
			System.out.println(" ****** Query Did not Return ********** ");
		}catch (Exception ex){
			ex.printStackTrace();
		}
		return null;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	private void formChordsRing() {
		try {
			int tempRingSize = 0;
			int i = 0;
			for (String portnum : Nodes) {
				String portKey = genHash(portnum);
				tempRingSize = chordRing.size();
				for (i = 0; i < tempRingSize; i++) {
					if (portKey.compareTo(chordRing.get(i)) <= 0) {
						chordRing.add(i, portKey);
						break;
					}
				}
				if (i == tempRingSize) {
					chordRing.add(portKey);
				}
				chordsKeyMap.put(portKey, String.valueOf(Integer.valueOf(portnum) * 2));
			}
			chordRingIndex = chordRing.indexOf(genHash(String.valueOf(Integer.valueOf(myPort) / 2)));
		} catch (Exception ex) {
			Log.e(TAG, "Exception ---->  " + ex.getMessage());
			ex.printStackTrace();
		}
		return;
	}

	private String getOperationNode(String keyHash) {
		try {
			if (chordRing.size() == 0) {
				return node_id;
			}
			String operationNode = "";
			for (String nodeKey : chordRing) {
				if (nodeKey.compareTo(keyHash) > 0) {
					operationNode = nodeKey;
					break;
				}
			}
			if (operationNode == "") {
				return chordRing.get(0);
			}
			return operationNode;
		} catch (Exception ex) {
			Log.e(TAG, " Exception --->  " + ex.getMessage() + "  " + Log.getStackTraceString(ex) );
			ex.printStackTrace();
			return null;
		}
	}

	private class RecoveryManager extends AsyncTask<String, String, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			isRecoveryOn = true;
			Socket socket = null;
			ObjectOutputStream out = null;
			Boolean first = true;
			System.out.println("######Recovery#####  INSIDE RECOVERY!!! ");
			List<String> predeccessors = getPredecessors(myPort);
			System.out.println("#####Recovery##  My Port --> " + myPort);
			System.out.println(" #####Recovery##  Predecessors --> " + predeccessors);
			List<String> successors = getSuccessors(myPort);
			System.out.println(" #####Recovery##  Successors --> " + successors);
			for (String remotePort : successors) {
				try {
					ObjectInputStream in = null;
					Object receivedObject;
					RecoveryClass recoveryResponse;
					System.out.println("#####Recovery## Remote Port -->  " + remotePort);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					RecoveryClass recoveryReq = new RecoveryClass();
					recoveryReq.isRecoveryRequest = true;
					recoveryReq.isReplicaRecovery = false;
					if(first){
						System.out.println("#####Recovery## Port is First Succ " + remotePort);
						first = false;
						recoveryReq.isFirstSuccessor = true;
					}else {
						recoveryReq.isFirstSuccessor = false;
					}
					recoveryReq.firstPredeccessor = predeccessors.get(0);
					recoveryReq.secondPredeccessor = predeccessors.get(1);
					recoveryReq.portnum = myPort;
					out.writeObject(recoveryReq);
					out.flush();
					while (!socket.isClosed()) {
						in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
						receivedObject = in.readObject();
						if (receivedObject != null) {
							recoveryResponse = (RecoveryClass) receivedObject;
							System.out.println("###Recovery###  Response Received ####  " + recoveryResponse.portnum);
							if(recoveryResponse.isRecoveryRequest != null) {
								if (recoveryResponse.isRecoveryRequest) {
									if (recoveryResponse.recoveryDataMap != null) {
										if (databaseMap.size() > 0) {
											databaseMap.putAll(recoveryResponse.recoveryDataMap);
										} else {
											databaseMap = recoveryResponse.recoveryDataMap;
										}
										System.out.println("######Recovery###  DataMap Received Size  " + recoveryResponse.recoveryDataMap.size()  + " Total Size:  " + databaseMap.size());
										if(recoveryResponse.versioningMap != null){
											for(String key : databaseMap.keySet()){
												if(recoveryResponse.versioningMap.containsKey(key)){
													/*if(keyVersioningMap.containsKey(key)){
														if(keyVersioningMap.get(key) <= recoveryResponse.versioningMap.get(key)){
															keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
														}
													}else {
														keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
													}*/
													keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
												}
											}
										}
									}
									if(recoveryResponse.isFirstSuccessor != null) {
										if (recoveryResponse.isFirstSuccessor && recoveryResponse.backupDataMap != null) {
											if (!predeccessorDatabaseMap.containsKey(predeccessors.get(0))) {
												predeccessorDatabaseMap.put(predeccessors.get(0), recoveryResponse.backupDataMap);
											} else {
												Map<String, String> preMap = predeccessorDatabaseMap.get(predeccessors.get(0));
												preMap.putAll(recoveryResponse.backupDataMap);
												predeccessorDatabaseMap.put(predeccessors.get(0), preMap);
											}
											System.out.println("######Recovery###  BackUpMap Received Size  " +  recoveryResponse.backupDataMap.size()  + " Total Size:  " + predeccessorDatabaseMap.get(predeccessors.get(0)).size());
											if(recoveryResponse.versioningMap != null){
												for(String key : predeccessorDatabaseMap.get(predeccessors.get(0)).keySet()){
													if(recoveryResponse.versioningMap.containsKey(key)){
														keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
													}
												}
											}
										}
									}
								}
							}
							break;
						}
					}
				} catch (IOException e) {
					Log.e(TAG, "SendTask socket IO Exception Recovery");
					Log.e(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} catch (Exception e) {
					Log.e(TAG, "SendTask socket Class Exception Recovery");
					Log.e(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} finally {
					try {
						out.close();
						socket.close();
					} catch (Exception ex) {
						Log.d(TAG, "Exception-->  " + ex.getMessage());
						ex.printStackTrace();
					}
				}
			}
			for (String remotePort : predeccessors) {
				try {
					ObjectInputStream in = null;
					Object receivedObject;
					RecoveryClass recoveryResponse;
					System.out.println("#####Recovery## Port -->  " + remotePort);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					RecoveryClass recoveryReq = new RecoveryClass();
					recoveryReq.isReplicaRecovery = true;
					recoveryReq.isRecoveryRequest = false;
					if(first){
						first = false;
						recoveryReq.isFirstPredeccessor = true;
					} else {
						recoveryReq.isFirstPredeccessor = false;
					}
					recoveryReq.firstPredeccessor = predeccessors.get(0);
					recoveryReq.secondPredeccessor = predeccessors.get(1);
					recoveryReq.portnum = myPort;
					out.writeObject(recoveryReq);
					out.flush();
					System.out.println("#####Recovery### Object Sent -->  ");
					while (!socket.isClosed()) {
						in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
						receivedObject = in.readObject();
						if (receivedObject != null) {
							recoveryResponse = (RecoveryClass) receivedObject;
							System.out.println("###Recovery### Response Replica Received ####  " + recoveryResponse.portnum);
							if (recoveryResponse.isReplicaRecovery) {
								if(recoveryResponse.recoveryDataMap != null) {
									if (!predeccessorDatabaseMap.containsKey(recoveryResponse.portnum)) {
										predeccessorDatabaseMap.put(recoveryResponse.portnum, recoveryResponse.recoveryDataMap);
									} else {
										Map<String, String> preMap = predeccessorDatabaseMap.get(recoveryResponse.portnum);
										preMap.putAll(recoveryResponse.recoveryDataMap);
										predeccessorDatabaseMap.put(recoveryResponse.portnum, preMap);
									}
									System.out.println("######Recovery###  Replica Map Received Size  " + recoveryResponse.recoveryDataMap.size()  + " Total Size:  " + predeccessorDatabaseMap.get(recoveryResponse.portnum).size());
									if(recoveryResponse.versioningMap != null){
										for(String key : predeccessorDatabaseMap.get(recoveryResponse.portnum).keySet()){
											if(recoveryResponse.versioningMap.containsKey(key)){
												keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
											}
										}
									}
								}
								if(recoveryResponse.isFirstPredeccessor != null && recoveryResponse.backupDataMap != null){
									if(recoveryResponse.isFirstPredeccessor) {
										if (!predeccessorDatabaseMap.containsKey(predeccessors.get(1))) {
											predeccessorDatabaseMap.put(predeccessors.get(1), recoveryResponse.backupDataMap);
										} else {
											Map<String, String> preMap = predeccessorDatabaseMap.get(predeccessors.get(1));
											preMap.putAll(recoveryResponse.backupDataMap);
											predeccessorDatabaseMap.put(predeccessors.get(1), preMap);
										}
										System.out.println("######Recovery###  BackUpMap Received Size  " +  recoveryResponse.backupDataMap.size()  + " Total Size:  " + predeccessorDatabaseMap.get(predeccessors.get(1)).size());
										if(recoveryResponse.versioningMap != null){
											for(String key : predeccessorDatabaseMap.get(predeccessors.get(1)).keySet()){
												if(recoveryResponse.versioningMap.containsKey(key)){
													keyVersioningMap.put(key, recoveryResponse.versioningMap.get(key));
												}
											}
										}
									}
								}
							}
							break;
						}
					}
				} catch (IOException e) {
					Log.e(TAG, "Recovery IO Exception Recovery");
					Log.e(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} catch (Exception e) {
					Log.e(TAG, "Recovery Class Exception Recovery");
					Log.e(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
					e.printStackTrace();
				} finally {
					try {

						out.close();
						socket.close();
					} catch (Exception ex) {
						Log.d(TAG, "Exception-->  " + ex.getMessage());
						ex.printStackTrace();
					}
				}
			}
			isRecoveryOn = false;
			return null;
		}
	}
}