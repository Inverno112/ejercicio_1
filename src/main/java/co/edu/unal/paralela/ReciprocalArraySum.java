package co.edu.unal.paralela;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de un arreglo usando paralelismo.
 */
public final class ReciprocalArraySum {
    private static final ForkJoinPool POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    /**
     * Constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Calcula secuencialmente la suma de valores recíprocos para un arreglo.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Calcula la suma de los recíprocos de los elementos del arreglo
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * calcula el tamaño de cada trozo o sección, de acuerdo con el número de secciones para crear
     * a través de un número dado de elementos.
     *
     * @param nChunks El número de secciones (chunks) para crear
     * @param nElements El número de elementos para dividir
     * @return El tamaño por defecto de la sección (chunk)
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Función techo entera
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Calcula el índice del elemento inclusivo donde la sección/trozo (chunk) inicia,
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk la sección/trozo (chunk) para cacular la posición de inicio
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice inclusivo donde esta sección/trozo (chunk) inicia en el conjunto de 
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Calcula el índice del elemento exclusivo que es proporcionado al final de la sección/trozo (chunk),
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk La sección para calcular donde termina
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice de terminación exclusivo para esta sección/trozo (chunk)
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * Tarea que suma los recíprocos de UN chunk (el que le corresponde según su taskId)
     * y delega el resto de chunks en un árbol binario de tareas: la tarea i crea
     * como hijos a las tareas 2i+1 y 2i+2. Los hijos con índice >= numTasks no corresponden
     * a ningún chunk y retornan 0.0 de inmediato (caso base).
     */
    private static class ReciprocalArraySumTask extends RecursiveTask<Double> {
        /** Número de esta tarea (también es el número de chunk que procesa). */
        private final int taskId;
        /** Cantidad total de tareas/chunks. */
        private final int numTasks;
        /** Arreglo de entrada para la suma de recíprocos. */
        private final double[] input;
        private final int chunkSize;

        /**
         * Constructor.
         *
         * @param setTaskId número de esta tarea, en [0, setNumTasks-1]
         * @param chunkSize el tamaño de cada chunk
         * @param setNumTasks cantidad total de tareas
         * @param setInput valores de entrada
         */
        ReciprocalArraySumTask(final int setTaskId, final int chunkSize, final int setNumTasks,
                final double[] setInput) {
            this.taskId = setTaskId;
            this.numTasks = setNumTasks;
            this.input = setInput;
            this.chunkSize = chunkSize;
        }

        @Override
        protected Double compute() {
            // Caso base: esta tarea no corresponde a ningún chunk (taskId >= numTasks).
            if (numTasks <= taskId) {
                return 0.0;
            }

            // Hijos del árbol: 2i+1 (izquierdo) y 2i+2 (derecho). Se crean siempre;
            // los que quedan fuera de rango retornan 0.0 por el caso base.
            ReciprocalArraySumTask left = new ReciprocalArraySumTask(2 * taskId + 1, chunkSize, numTasks, input);
            ReciprocalArraySumTask right = new ReciprocalArraySumTask(2 * taskId + 2, chunkSize, numTasks, input);
            left.fork();
            right.fork();

            // Suma del chunk propio (los límites se calculan una sola vez, en el encabezado del for).
            double sum = 0;
            int start = taskId * chunkSize;
            for (int i = start,
                    end = Math.min(start + chunkSize, input.length); i < end; i++) {
                sum += 1 / input[i];
            }

            return sum + left.join() + right.join();
        }
    }

    /**
     * Calcula la misma suma de recíprocos que seqArraySum, pero usando dos tareas
     * en paralelo dentro del framework ForkJoin de Java.
     * Se puede asumir que el largo del arreglo de entrada es igualmente divisible por 2.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;
        return parManyTaskArraySum(input, 2);
    }

    /**
     * Calcula la suma de recíprocos usando un número establecido de tareas.
     *
     * @param input Arreglo de entrada
     * @param numTasks El número de tareas para crear
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        // La tarea 0 es la raíz del árbol y crea (recursivamente) a todas las demás.
        return POOL.invoke(new ReciprocalArraySumTask(0, getChunkSize(numTasks, input.length), numTasks, input));
        }
}