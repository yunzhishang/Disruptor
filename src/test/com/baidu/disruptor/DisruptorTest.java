package com.baidu.disruptor;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.rigel.profiler.Profiler;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorTest {

	@SuppressWarnings("unchecked")
    public static void main(String[] args) {
        ExecutorService exec = Executors.newCachedThreadPool();
        // Preallocate RingBuffer with 1024 ValueEvents
        Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 4, exec);
        final EventHandler<ValueEvent> handler = new EventHandler<ValueEvent>() {
            // event will eventually be recycled by the Disruptor after it wraps
            public void onEvent(final ValueEvent event, final long sequence, final boolean endOfBatch) throws Exception {
                System.out.println("Sequence: " + sequence);
                System.out.println("ValueEvent: " + event.getValue());
            }
        };
        // Build dependency graph
        disruptor.handleEventsWith(handler);
        RingBuffer<ValueEvent> ringBuffer = disruptor.start();

        for (long i = 0; i < 1; i++) {
            String uuid = UUID.randomUUID().toString();
            // Two phase commit. Grab one of the 1024 slots
            long seq = ringBuffer.next();
            ValueEvent valueEvent = ringBuffer.get(seq);
            valueEvent.setValue(uuid);
            ringBuffer.publish(seq);
        }
        disruptor.shutdown();
        exec.shutdown();
    }
	
	public static void main1(String[] args) {
		long l1 = Profiler.start(); 
		Object[] arrayOfObject = new Object[1]; 
		arrayOfObject[0] = args; 
		Profiler.xxxx(arrayOfObject);
        ExecutorService exec = Executors.newCachedThreadPool();
        // Preallocate RingBuffer with 1024 ValueEvents
        Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 4, exec);
        final EventHandler<ValueEvent> handler = new EventHandler<ValueEvent>() {
            // event will eventually be recycled by the Disruptor after it wraps
            public void onEvent(final ValueEvent event, final long sequence, final boolean endOfBatch) throws Exception {
                System.out.println("Sequence: " + sequence);
                System.out.println("ValueEvent: " + event.getValue());
            }
        };
        // Build dependency graph
        disruptor.handleEventsWith(handler);
        RingBuffer<ValueEvent> ringBuffer = disruptor.start();

        for (long i = 0; i < 1; i++) {
            String uuid = UUID.randomUUID().toString();
            // Two phase commit. Grab one of the 1024 slots
            long seq = ringBuffer.next();
            ValueEvent valueEvent = ringBuffer.get(seq);
            valueEvent.setValue(uuid);
            ringBuffer.publish(seq);
        }
        disruptor.shutdown();
        exec.shutdown();
        Profiler.end(l1, "com.baidu.disruptor.DisruptorTest.main1");
    }
	
}
