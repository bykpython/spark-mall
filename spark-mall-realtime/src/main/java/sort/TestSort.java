package sort;

import org.junit.Test;

public class TestSort {

    @Test
    public void testSort(){
        int []arr = new int[]{3, 5, 9, 1, 2, 4, 9, 10, 8};

        System.out.println("before sort:");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();

//        QuickSort.quickSort(arr, 0, arr.length-1);
//        MergeSort1.sort(arr);
        BubbleSort.bubbleSort(arr);
        System.out.println("after sort:");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();
    }
}
