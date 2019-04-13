package sort;

import org.junit.Test;

public class TestSort {

    @Test
    public void testSort() {

        int[] arr = new int[]{2, 4, 6, 9, 8, 10, 1, 45, 23, 57, 34, 55};

        System.out.println("排序前:");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();

        //QuickSort.quickSort(arr, 0, arr.length-1);
        MergeSort2.sort(arr);

        System.out.println("排序后:");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();

    }
}
