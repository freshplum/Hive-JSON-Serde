/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/


package org.openx.data.arraysum;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

public class ArraySum extends UDF {

    public DoubleWritable evaluate(ArrayList<Double> list) {
        DoubleWritable result = new DoubleWritable(-1);
        if (list == null || list.size() < 1) {
            return result;
        }

        Double sum = 0.0;
        for (Double i : list) {
            sum += i;
        }
        result.set(sum);
        return result;
    }
}
