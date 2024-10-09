package tukano.impl.storage;

import java.util.function.Consumer;

import tukano.api.Result;

public interface BlobStorage {
		
	public Result<Void> write(String id, byte[] bytes );
		
	public Result<Void> delete(String id);
	
	public Result<byte[]> read(String id);

	public Result<Void> read(String id, Consumer<byte[]> sink);

}
