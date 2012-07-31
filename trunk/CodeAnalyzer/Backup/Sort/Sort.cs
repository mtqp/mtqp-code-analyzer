using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer
{
    public class Sort
    {
        public static List<T> IdiotSort<T>(List<T> list) where T : ISortable
        {
            int minIndex = 0;
            for (int index=0; index < list.Count; index++)
            {
                minIndex = GetMinIndex<T>(index, list);
                Swap<T>(index, minIndex, list);
            }
            return list;
        }

        private static void Swap<T>(int index, int minIndex, List<T> list)
        {
            T intermmediate = list[index];
            list[index] = list[minIndex];
            list[minIndex] = intermmediate;
        }

        private static int GetMinIndex<T>(int startIndex, List<T> list) where T : ISortable
        {
            int minElement = list[startIndex].Id;
            int minIndex = startIndex;

            for (int index = startIndex; index < list.Count; index++)
            {
                minElement = Math.Min(minElement, list[index].Id);
                if (minElement == list[index].Id)
                    minIndex = index;
            }

            return minIndex;
        }

    }
}
