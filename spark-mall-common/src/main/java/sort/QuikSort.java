package sort;

public class QuikSort {

    public static void quickSort(int[] arr, int left, int right) {

        if (left > right) {
            return;
        }
        int i = left;
        int j = right;
        int temp = arr[i]; // 选择基准数

        while (i != j && i < j) {

            while (arr[j] > temp && i < j) {
                j = j - 1;
            }

            if (i < j){
                arr[i] = arr[j];
            }

            while (arr[i] <= temp && i < j) {
                i = i + 1;
            }

            if (i < j) {
                arr[j] = arr[i];
            }
        }

        // 最终将基准数归位
        arr[i] = temp;

        // 递归排序基数以左的序列
        quickSort(arr, left, i-1);
        // 递归排序基数以右的序列
        quickSort(arr, i+1, right);
    }
}
