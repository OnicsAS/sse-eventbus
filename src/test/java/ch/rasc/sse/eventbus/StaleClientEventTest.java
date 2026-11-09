package ch.rasc.sse.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import ch.rasc.sse.eventbus.config.SseEventBusConfigurer;

/**
 * Events queued for a client that has since been unregistered, or replaced by a new
 * registration with the same id, must not reach or affect the id's current client.
 */
public class StaleClientEventTest {

	private static final String CLIENT_ID = "client";

	private static final String EVENT = "home";

	private static final Duration SCHEDULER_DELAY = Duration.ofMillis(50);

	private final CountDownLatch sendGate = new CountDownLatch(1);

	private final BlockingQueue<ClientEvent> sendQueue = new LinkedBlockingQueue<ClientEvent>() {
		private static final long serialVersionUID = 1L;

		@Override
		public ClientEvent take() throws InterruptedException {
			StaleClientEventTest.this.sendGate.await();
			return super.take();
		}
	};

	private final SseEventBus eventBus = new SseEventBus(new SseEventBusConfigurer() {
		@Override
		public int noOfSendResponseTries() {
			return 1; // one failed send unregisters the client
		}

		@Override
		public Duration schedulerDelay() {
			return SCHEDULER_DELAY;
		}

		@Override
		public BlockingQueue<ClientEvent> sendQueue() {
			return StaleClientEventTest.this.sendQueue;
		}
	}, new DefaultSubscriptionRegistry(), null);

	@After
	public void tearDown() {
		this.sendGate.countDown();
		this.eventBus.cleanUp();
	}

	@Test
	public void aFailedEventForAReplacedClientKeepsItsSuccessor() throws InterruptedException {
		SseEmitter previousEmitter = new SseEmitter();
		this.eventBus.registerClient(CLIENT_ID, previousEmitter);
		this.eventBus.subscribe(CLIENT_ID, EVENT);
		this.eventBus.handleEvent(
				SseEvent.builder().addClientId(CLIENT_ID).event(EVENT).data("event").build());

		// The client subscribes again: a new registration replaces the old one, and
		// the old emitter is closed, so a send to it fails.
		this.eventBus.unregisterClient(CLIENT_ID);
		this.eventBus.registerClient(CLIENT_ID, new SseEmitter());
		this.eventBus.subscribe(CLIENT_ID, EVENT);
		previousEmitter.complete();

		this.sendGate.countDown();
		awaitEmptySendQueue();
		Thread.sleep(SCHEDULER_DELAY.multipliedBy(10).toMillis());

		assertThat(this.eventBus.getAllClientIds()).containsExactly(CLIENT_ID);
		assertThat(this.eventBus.getSubscribers(EVENT)).containsExactly(CLIENT_ID);
	}

	@Test
	public void anEventForAnUnregisteredClientIdIsNotQueued() {
		this.eventBus.subscribe(CLIENT_ID, EVENT);

		this.eventBus.handleEvent(
				SseEvent.builder().addClientId(CLIENT_ID).event(EVENT).data("event").build());

		assertThat(this.sendQueue).isEmpty();
	}

	@Test
	public void anEventForTheRegisteredClientIsQueued() {
		this.eventBus.registerClient(CLIENT_ID, new SseEmitter());
		this.eventBus.subscribe(CLIENT_ID, EVENT);

		this.eventBus.handleEvent(SseEvent.builder()
				.clientIds(Collections.singleton(CLIENT_ID)).event(EVENT).data("event").build());

		assertThat(this.sendQueue).hasSize(1);
	}

	private void awaitEmptySendQueue() throws InterruptedException {
		long deadline = System.currentTimeMillis() + 2000;
		while (!this.sendQueue.isEmpty() && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}
		assertThat(this.sendQueue).isEmpty();
	}
}
