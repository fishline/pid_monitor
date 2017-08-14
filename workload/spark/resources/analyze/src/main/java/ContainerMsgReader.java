/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

/*
 * Return the error message together with container info
 */
public class ContainerMsgReader extends RecordReader<Text, Text> {
    private long start = 0L;
    private long end = 0L;
    private long pos = 0L;
    private LineReader reader = null;
    private Text key = new Text();
    private Text value = new Text();
    private Text buf = new Text();
    private boolean initInfo = true;
    private boolean resetKeyVal = false;
    private Text cid = null;
    private boolean foundTgt = false;
    private boolean isStdout = false;

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    /*
     * The first key/value pair returns "containerID:SUMMARY"/"filename:hottub_info:(map/reduce/zero/unknown):duration"
     * Prepare the information during initialize
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)inputSplit;
        FSDataInputStream fileIn = split.getPath().getFileSystem(context.getConfiguration()).open(split.getPath());
        start = 0L;
        end = start + split.getLength();
        fileIn.seek(start);
        reader = new LineReader(fileIn, context.getConfiguration());
        pos = start;

        String pathStr = split.getPath().toString();
        Pattern pCID = Pattern.compile("container\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+");
        Pattern pFN = Pattern.compile("\\p{Punct}[a-zA-Z]+$");
        Pattern pHOTTUB = Pattern.compile("hottub_info-tdw-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+");
        Matcher mCID = pCID.matcher(pathStr);
        Matcher mFN = pFN.matcher(pathStr);
        Matcher mHOTTUB = pHOTTUB.matcher(pathStr);

        // Check for type and duration info
        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;
        boolean found4 = false;
        long startTS = 0L;
        long stopTS = 0L;
        while (pos < end) {
            buf = new Text();
            pos = pos + reader.readLine(buf, Integer.MAX_VALUE, Integer.MAX_VALUE);

            Pattern pt1 = Pattern.compile("^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+");
            Matcher mt1 = pt1.matcher(buf.toString());
            if (mt1.find()) {
                SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date d1 = datetimeFormatter1.parse(mt1.group());
                    if (d1 == null) {
                        datetimeFormatter1 = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
                        d1 = datetimeFormatter1.parse(mt1.group());
                    }
                    if (d1 != null) {
                        if (startTS == 0) {
                            startTS = d1.getTime();
                        }
                        stopTS = d1.getTime();
                    }
                } 
                catch (ParseException e)
                {
                    try {
                        datetimeFormatter1 = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
                        Date d1 = datetimeFormatter1.parse(mt1.group());
                        if (d1 != null) {
                            if (startTS == 0) {
                                startTS = d1.getTime();
                            }
                            stopTS = d1.getTime();
                        }
                    }
                    catch (ParseException e2)
                    {

                    }
                }
            }
            Pattern p1 = Pattern.compile("MapTask\\s+metrics\\s+system");
            Pattern p2 = Pattern.compile("org.apache.hadoop.mapred.MapTask");
            Pattern p3 = Pattern.compile("ReduceTask\\s+metrics\\s+system");
            Pattern p4 = Pattern.compile("org.apache.hadoop.mapreduce.task.reduce");
            Matcher m1 = p1.matcher(buf.toString());
            Matcher m2 = p2.matcher(buf.toString());
            Matcher m3 = p3.matcher(buf.toString());
            Matcher m4 = p4.matcher(buf.toString());
            if (m1.find()) {
                found1 = true;
            }
            if (m2.find()) {
                found2 = true;
            }
            if (m3.find()) {
                found3 = true;
            }
            if (m4.find()) {
                found4 = true;
            }
        }
        reader.close();
        buf = new Text();

        String typeInfo = new String("NA");
        boolean mfnFind = mFN.find();
        if (mfnFind && mFN.group().equals("/syslog")) {
            if (split.getLength() == 0) {
                typeInfo = new String("MR-zero");
            } else {
                if (found1 == true || found2 == true) {
                    typeInfo = new String("MR-map");
                } else if (found3 == true || found4 == true) {
                    typeInfo = new String("MR-reduce");
                } else {
                    typeInfo = new String("MR-unknown");
                }
            }
        }

        if (mfnFind && mFN.group().equals("/stdout")) {
            isStdout = true;
        }

        // Assemble info into key/value pair
        if ((!mCID.find()) || (!mfnFind) || (!mHOTTUB.find())) {
            key = new Text("MISSING_INFO");
            value = new Text("MISSING_INFO");
            cid = new Text("MISSING_INFO");
        } else {
            key = new Text(mCID.group() + ":SUMMARY");
            cid = new Text(mCID.group());
            value = new Text(mFN.group() + ":" + mHOTTUB.group() + ":" + typeInfo + ":" + Long.toString((stopTS - startTS)/1000L));
        }

        fileIn = split.getPath().getFileSystem(context.getConfiguration()).open(split.getPath());
        start = 0L;
        end = start + split.getLength();
        fileIn.seek(start);
        reader = new LineReader(fileIn, context.getConfiguration());
        pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (initInfo == true) {
            initInfo = false;
            resetKeyVal = true;
            return true;
        } else {
            if (resetKeyVal == true) {
                resetKeyVal = false;
                key = new Text();
                value = new Text();
            }
        }
        key = cid;
        if (isStdout == false) {
            if (foundTgt == true && buf.getLength() > 0) {
                value = buf;
                if (pos >= end) {
                    foundTgt = false;
                    return true;
                }
            } else {
                value = new Text();
            }
            if (pos >= end) {
                return false;
            }
            while (pos < end) {
                // read next line
                buf = new Text();
                pos = pos + reader.readLine(buf, Integer.MAX_VALUE, Integer.MAX_VALUE);
                Pattern p1 = Pattern.compile("^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+");
                Matcher m1 = p1.matcher(buf.toString());
                if (!m1.find()) {
                    // Not new line, append to value if current block is the one we are pursuing
                    if (foundTgt == true) {
                        value = new Text(value + " " + buf);
                    }
                } else {
                    // get new line started with date
                    // Check with the line to search for " ERROR "/" WARN "/" FATAL "/"executor.Executor: Executor killed task"/"executor.Executor: Executor is trying to kill task"
                    Pattern pError = Pattern.compile("(\\s+ERROR\\s+)|(\\s+WARN\\s+)|(\\s+FATAL\\s+)|(executor\\p{Punct}Executor\\p{Punct}\\s+Executor\\s+killed\\s+task)|(executor\\p{Punct}Executor\\p{Punct}\\s+Executor\\s+is\\s+trying\\s+to\\s+kill\\s+task)");
                    Matcher mError = pError.matcher(buf.toString());
                    boolean foundCurrent = false;
                    if (mError.find()) {
                        foundCurrent = true;
                    } else {
                        foundCurrent = false;
                    }

                    if (foundTgt == true) {
                        // Previous block is target, return that to user
                        foundTgt = foundCurrent;
                        return true;
                    } else {
                        foundTgt = foundCurrent;
                        if (foundTgt == true) {
                            // Current block is target, save buf to value
                            value = buf;
                        } else {
                            value = new Text();
                        }
                    }
                }
            }
            if (foundTgt == true) {
                foundTgt = false;
                return true;
            }
        } else {
            while (pos < end) {
                // read next line
                buf = new Text();
                pos = pos + reader.readLine(buf, Integer.MAX_VALUE, Integer.MAX_VALUE);
                Pattern p1 = Pattern.compile("java.lang.OutOfMemoryError");
                Matcher m1 = p1.matcher(buf.toString());
                if (m1.find()) {
                    value = buf;
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }
}

