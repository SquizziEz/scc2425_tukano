package tukano.impl.storage;


import static tukano.api.Result.error;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;

import tukano.api.Result;
import utils.Hash;
import utils.IO;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;


public class CloudSystemStorage implements BlobStorage {
	private final String rootDir;
	private static final int CHUNK_SIZE = 4096;
	private static final String DEFAULT_ROOT_DIR = "/tmp/";

	private static final String BLOBS_CONTAINER_NAME = "blobs";
	String storageConnectionString = "";
	BlobContainerClient containerClient = new BlobContainerClientBuilder()
				.connectionString(storageConnectionString)
				.containerName(BLOBS_CONTAINER_NAME)
				.buildClient();




	public CloudSystemStorage() {
		this.rootDir = DEFAULT_ROOT_DIR;
	}
	
	@Override
	public Result<Void> write(String id, byte[] bytes) {
		if (id == null)
			return error(BAD_REQUEST);

		var key = Hash.of(bytes);
		BinaryData data = BinaryData.fromBytes(bytes);
		BlobClient blob = containerClient.getBlobClient( key);

		if (key == null) {
			/**if (Arrays.equals(Hash.sha256(bytes), Hash.sha256(IO.read(file))))
				return ok();
			else*/
				return error(CONFLICT);

		}
		blob.upload(data);
		return ok();
	}

	@Override
	public Result<byte[]> read(String id) {
		if (id == null)
			return error(BAD_REQUEST);
		
		/**var file = toFile( path );
		if( ! file.exists() )
			return error(NOT_FOUND);*/
		var key = Hash.of(id);
		BlobClient blob = containerClient.getBlobClient( key);
		BinaryData data = blob.downloadContent();
		byte[] bytes = data.toBytes();
		return bytes != null ? ok( bytes ) : error( INTERNAL_ERROR );
	}

	@Override
	public Result<Void> read(String id, Consumer<byte[]> sink) {
		if (id == null)
			return error(BAD_REQUEST);
		
		/**var file = toFile( path );
		if( ! file.exists() )
			return error(NOT_FOUND);*/
		
		//IO.read( file, CHUNK_SIZE, sink );
		var key = Hash.of(id);
		BlobClient blob = containerClient.getBlobClient( key);
		BinaryData data = blob.downloadContent();
		byte[] bytes = data.toBytes();
		if( bytes == null )
			return error( INTERNAL_ERROR );
		
		sink.accept( bytes );
		return ok();
	}
	
	@Override
	public Result<Void> delete(String id) {
		if (id == null)
			return error(BAD_REQUEST);

		/**try {
			var file = toFile( path );
			Files.walk(file.toPath())
			.sorted(Comparator.reverseOrder())
			.map(Path::toFile)
			.forEach(File::delete);
		} catch (IOException e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}*/
		var key = Hash.of(id);
		BlobClient blob = containerClient.getBlobClient( key);
		blob.delete();
		return ok();
	}
	
	private File toFile(String path) {
		var res = new File( rootDir + path );
		
		var parent = res.getParentFile();
		if( ! parent.exists() )
			parent.mkdirs();
		
		return res;
	}
}
