package sort;

import java.util.Arrays;

public class BubbleSort {

    public static void bubbleSort(int []arr){

        int arrLen = arr.length;

        for(int i = 0; i < arrLen-1; i++){

            boolean flag = false;

            for(int j = 0; j < arrLen-1-i; j++){

                if(arr[j] > arr[j+1]){

                    int temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;
                    flag = true;
                }
            }

            //System.out.println(Arrays.toString(arr));

            if(!flag){
                break;
            }
        }
    }

    public static void bubbleSort2(int[] data) {

        System.out.println("开始排序");
        int arrayLength = data.length;

        for (int i = 0; i < arrayLength - 1; i++) {

            boolean flag = false;

            for (int j = 0; j < arrayLength - 1 - i; j++) {
                if(data[j] > data[j + 1]){
                    int temp = data[j + 1];
                    data[j + 1] = data[j];
                    data[j] = temp;
                    flag = true;
                }
            }

            System.out.println(java.util.Arrays.toString(data));

            if (!flag)
                break;
        }
    }

}
