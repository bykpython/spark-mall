package sort;

import org.junit.Test;

public class SortTest {

    @Test
    public void testSort() {
        int[] arr = new int[]{5, 8, 7, 6, 1, 0, -1, 9, 5, 6};

        System.out.print("排序前：");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();
        MergeSort.sort(arr);
//        QuikSort.quickSort(arr, 0, arr.length - 1);
        System.out.print("排序后：");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();
    }
}
