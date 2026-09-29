package co.edu.unal.paralela;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de un arreglo usando paralelismo.
 */
public final class ReciprocalArraySum {

    private static final ForkJoinPool POOL = ForkJoinPool.commonPool();

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
     * Este pedazo de clase puede ser completada para para implementar el cuerpo de cada tarea creada
     * para realizar la suma de los recíprocos del arreglo en paralelo.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**         * Iniciar el índice para el recorrido transversal hecho por esta tarea.         */
        private final int startIndexInclusive;/**   * Concluir el índice para el recorrido transversal hecho por esta tarea.         */
        private final int endIndexExclusive;        /**         * Arreglo de entrada para la suma de recíprocos.         */
        private final double[] input;        /**         * Valor intermedio producido por esta tarea.         */
        protected double value = 0;
        private final int numTasks;
        /**         * Constructor.
         * * @param setStartIndexInclusive establece el índice inicial para comenzar
         *        el recorrido trasversal.
         *        * @param setEndIndexExclusive establece el índice final para el recorrido trasversal.
         * @param setInput Valores de entrada
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput, final int tSize) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.numTasks=tSize;
        }

        /**
         * Adquiere el valor calculado por esta tarea.
         * @return El valor calculado por esta tarea
         */
        public double getValue() {
            return this.value;
        }
        /* @Override
        protected void compute() {
            if (endIndexExclusive - startIndexInclusive == numTasks) {
                double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
                return;
            }
            final ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
            final int chunkSize = getChunkSize(numTasks, input.length);
            for (int i = 1; i < numTasks; i++) {
                tasks[i] = new ReciprocalArraySumTask(
                        getChunkStartInclusive(i, numTasks, input.length),
                        getChunkEndExclusive(i, numTasks, input.length), input, chunkSize);
                tasks[i].fork();
            }
            tasks[0] = new ReciprocalArraySumTask(
                    getChunkStartInclusive(0, numTasks, input.length),
                    getChunkEndExclusive(0, numTasks, input.length), input, chunkSize);
            tasks[0].compute();
            for (int i = 1; i < numTasks; i++) {
                tasks[i].join();
            }
            for (ReciprocalArraySumTask task : tasks) {
                value += task.getValue();
            }
        } */
       //Versión recursiva
       /* @Override 
       protected void compute() {

            if (numTasks == 1) { // Umbral para dividir
                   double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                 ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(0, getChunkEndExclusive(numTasks-2, numTasks, endIndexExclusive), input, numTasks-1);
                
                rightTask.fork(); // Ejecuta la tarea derecha en el hilo actual

                double parcial = 0;
                for (int i = getChunkStartInclusive(numTasks-1, numTasks, endIndexExclusive); i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                rightTask.join(); // Espera a que la tarea derecha termine

                value = parcial + rightTask.getValue(); // Suma los resultados                
            }
        }*/
       /* 
       //Más eficiente que la versión recursiva, pero no es la más eficiente
       @Override 
       protected void compute() {

            if (endIndexExclusive - startIndexInclusive <= getChunkSize(numTasks, input.length)) { // Umbral para dividir
                   double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                 int mid = (startIndexInclusive + endIndexExclusive) / 2;
                 ReciprocalArraySumTask leftTask = new ReciprocalArraySumTask(startIndexInclusive, mid, input, numTasks);
                 ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(mid, endIndexExclusive, input, numTasks);
                
                leftTask.fork(); // Ejecuta la tarea izquierda en un hilo separado
                rightTask.compute(); // Ejecuta la tarea derecha en el hilo actual
                leftTask.join(); // Espera a que la tarea izquierda termine

                value = leftTask.getValue() + rightTask.getValue(); // Suma los resultados                
            }
        } */
       /* @Override
       protected void compute() {
            final int chunkSize = getChunkSize(numTasks, input.length);
            final int rangeSize = endIndexExclusive - startIndexInclusive;

            if (rangeSize <= chunkSize) {
                double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                final int chunkEndExclusive = startIndexInclusive + chunkSize;
                final ReciprocalArraySumTask chunkTask = new ReciprocalArraySumTask(
                        startIndexInclusive, chunkEndExclusive, input, numTasks);
                final ReciprocalArraySumTask remainingTask = new ReciprocalArraySumTask(
                        chunkEndExclusive, endIndexExclusive, input, numTasks);

                chunkTask.fork();
                remainingTask.compute();
                chunkTask.join();

                value = chunkTask.getValue() + remainingTask.getValue();
            }
        } */
     
        

       
    }

    /**
     * Para hacer: Modificar este método para calcular la misma suma de recíprocos como le realizada en
     * seqArraySum, pero utilizando dos tareas ejecutándose en paralelo dentro del framework ForkJoin de Java
     * Se puede asumir que el largo del arreglo de entrada 
     * es igualmente divisible por 2.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        final ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input, 2);

        //Acá qué se supone que hago?
        task.fork(); // Inicia la tarea en un hilo separado
        task.join(); // Espera a que la tarea termine
        
        /*
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }*/
        return task.getValue();
    }

    /**
     * Para hacer: extender el trabajo hecho para implementar parArraySum que permita utilizar un número establecido
     * de tareas para calcular la suma del arreglo recíproco. 
     * getChunkStartInclusive y getChunkEndExclusive pueden ser útiles para cacular 
     * el rango de elementos índice que pertenecen a cada sección/trozo (chunk).
     *
     * @param input Arreglo de entrada
     * @param numTasks El número de tareas para crear
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        final ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input, numTasks);

        //Acá qué se supone que hago?
       task.fork(); // Inicia la tarea en un hilo separado
       task.join(); // Espera a que la tarea termines

        return task.getValue();
    }
}
