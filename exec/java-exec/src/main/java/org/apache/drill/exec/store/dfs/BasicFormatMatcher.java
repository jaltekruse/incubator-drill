package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

public class BasicFormatMatcher extends FormatMatcher{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicFormatMatcher.class);

  private final List<Pattern> patterns;
  private final MagicStringMatcher matcher;
  private final FileSystem fs;
  
  public BasicFormatMatcher(FileSystem fs, List<Pattern> patterns, List<MagicString> magicStrings) {
    super();
    this.patterns = ImmutableList.copyOf(patterns);
    this.matcher = new MagicStringMatcher(magicStrings);
    this.fs = fs;
  }

  @Override
  public ReadHandle isDirReadable(FileStatus dir) {
    return null;
  }

  @Override
  public ReadHandle isFileReadable(FileStatus file) throws IOException {
    String path = file.getPath().toString();
    for(Pattern p : patterns){
      if(p.matcher(path).matches()){
        return new FileReadHandle(file);
      }
    }
    
    if(matcher.matches(file)) return new FileReadHandle(file);
    
    return null;
  }
  
  private class MagicStringMatcher{
    
    private List<RangeMagics> ranges;
    
    public MagicStringMatcher(List<MagicString> magicStrings){
      ranges = Lists.newArrayList();
      for(MagicString ms : magicStrings){
        ranges.add(new RangeMagics(ms));
      }
    }
    
    public boolean matches(FileStatus status) throws IOException{
      if(ranges.isEmpty()) return false;
      final Range<Long> fileRange = Range.closedOpen( 0L, status.getLen());
      
      try(FSDataInputStream is = fs.open(status.getPath())){
        for(RangeMagics rMagic : ranges){
          Range<Long> r = rMagic.range;
          if(!fileRange.encloses(r)) continue;
          int len = (int) (r.upperEndpoint() - r.lowerEndpoint());
          byte[] bytes = new byte[len];
          is.readFully(r.lowerEndpoint(), bytes);
          for(byte[] magic : rMagic.magics){
            if(Arrays.equals(magic, bytes)) return true;  
          }
          
        }
      }
      return false;
    }
    
    private class RangeMagics{
      Range<Long> range;
      byte[][] magics;
      
      public RangeMagics(MagicString ms){
        this.range = Range.closedOpen( ms.getOffset(), (long) ms.getBytes().length);
        this.magics = new byte[][]{ms.getBytes()};
      }
    }
  }
}
