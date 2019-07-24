package com.huawei.hwcloud.tarus.kvstore.race;

import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;

public class EngineKVStoreRace implements KVStoreRace {

	@Override
	public boolean init(final String dir, final int file_size) throws KVSException {
		return false;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {
		return 0;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {
		return 0;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void flush() {
		
	}
}
