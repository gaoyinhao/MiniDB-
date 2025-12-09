package top.gaoyinhao.mydb.backend.common;

import com.bupt.mydb.backend.common.SubArray;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/17 22:33
 * description:
 */
public class SubArrayTest {


    @Test
    public void testSubArray() {
        byte[] subArray = new byte[10];
        for (int i = 0; i < subArray.length;
             i++) {
            subArray[i] = (byte) (i + 1);
        }
        SubArray sub1 = new SubArray(subArray, 3, 7);
        SubArray sub2 = new SubArray(subArray, 6, 9);

        sub1.raw[4] = (byte) 44;
        System.out.println("Original Array: ");
        printArray(subArray);
        System.out.println("SubArray1: ");
        printSubArray(sub1);
        System.out.println("SubArray2: ");
        printSubArray(sub2);
    }

    private void printArray(byte[] array){
        System.out.println(Arrays.toString(array));
    }

    private void printSubArray(SubArray subArray){
        for (int i = subArray.start; i <= subArray.end; i++) {
            System.out.print(subArray.raw[i] + "\t");
        }
        System.out.println();
    }
}
