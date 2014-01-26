package org.apache.drill.exec.compile;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.exception.ClassTransformationException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Resources;

public class ByteCodeLoader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteCodeLoader.class);
  
  
  private final LoadingCache<String, byte[]> byteCode = CacheBuilder.newBuilder().maximumSize(10000)
      .expireAfterWrite(10, TimeUnit.MINUTES).build(new ClassBytesCacheLoader());

  private class ClassBytesCacheLoader extends CacheLoader<String, byte[]> {
    public byte[] load(String path) throws ClassTransformationException, IOException {
      URL u = this.getClass().getResource(path);
      if (u == null)
        throw new ClassTransformationException(String.format("Unable to find TemplateClass at path %s", path));
      return Resources.toByteArray(u);
    }
  };

  public byte[] getClassByteCodeFromPath(String path) throws ClassTransformationException, IOException {
    try {
      return byteCode.get(path);
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if (c instanceof ClassTransformationException)
        throw (ClassTransformationException) c;
      if (c instanceof IOException)
        throw (IOException) c;
      throw new ClassTransformationException(c);
    }
  }
}
