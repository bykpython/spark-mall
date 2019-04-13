package sort;

public class QuickSort{

    public static void quickSort(int []arr, int left, int right){

        if(left >= right){
            return;
        }

        int i = left;
        int j = right;
        int temp = arr[i];

        while(i != j){

            while(i < j && arr[j] >= temp){
                j--;
            }
            if(i < j){
                arr[i] = arr[j];
            }

            while(i < j && arr[i] < temp){
                i++;
            }
            if(i < j){
                arr[j] = arr[i];
            }
        }

        arr[i] = temp;

        quickSort(arr, left, i-1);
        quickSort(arr, i+1, right);
    }
}