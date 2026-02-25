/// Lesson 6: StreamController — Pushing Data Imperatively
///
/// StreamController<T> is the "write side" of a stream.
/// It gives you a Sink to push events and a Stream to expose to consumers.
///
/// Two flavours:
///   StreamController()            — single-subscription (default)
///   StreamController.broadcast()  — multiple listeners allowed
///
/// Lifecycle:
///   controller.add(event)         — push a data event
///   controller.addError(e)        — push an error event
///   controller.close()            — signal done
///   controller.stream             — the readable Stream<T>
///   controller.sink               — the writable StreamSink<T>

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── 1. Basic single-subscription controller ──────────────────────────────────

Future<void> basicControllerDemo() async {
  header('1. Basic StreamController (single-subscription)');

  final controller = StreamController<String>();

  // Expose the readable side to a listener
  final subscription = controller.stream.listen(
    (msg) => print('  received: $msg'),
    onDone: () => print('  stream closed'),
  );

  // Push events from anywhere (here, synchronously for brevity)
  controller.add('event A');
  controller.add('event B');
  controller.add('event C');
  await controller.close(); // sends the done event

  await subscription.asFuture<void>(); // wait until listener is done
}

// ─── 2. onListen / onCancel / onPause / onResume callbacks ───────────────────

Future<void> callbacksDemo() async {
  header('2. Controller callbacks: onListen, onPause, onResume, onCancel');

  late StreamController<int> controller;
  controller = StreamController<int>(
    onListen: () => print('  >> listener attached'),
    onPause: () => print('  >> listener paused'),
    onResume: () => print('  >> listener resumed'),
    onCancel: () => print('  >> listener cancelled'),
  );

  final sub = controller.stream.listen((v) => print('  data: $v'));
  controller.add(1);

  sub.pause(Future.delayed(Duration(milliseconds: 50)));
  controller.add(2); // buffered while paused
  await Future.delayed(Duration(milliseconds: 100)); // wait for resume
  controller.add(3);

  await sub.cancel();
  await controller.close();
}

// ─── 3. Checking controller state ────────────────────────────────────────────

Future<void> stateCheckDemo() async {
  header('3. Controller state flags');

  final c = StreamController<int>();
  print('  hasListener before listen: ${c.hasListener}');
  print('  isClosed before close:     ${c.isClosed}');

  final sub = c.stream.listen((_) {});
  print('  hasListener after listen:  ${c.hasListener}');

  await c.close();
  print('  isClosed after close:      ${c.isClosed}');

  await sub.cancel();
}

// ─── 4. Broadcast controller ─────────────────────────────────────────────────

Future<void> broadcastControllerDemo() async {
  header('4. Broadcast StreamController — multiple listeners');

  final controller = StreamController<String>.broadcast();

  // Attach two independent listeners
  final results1 = <String>[];
  final results2 = <String>[];

  final sub1 = controller.stream.listen((e) {
    results1.add(e);
    print('  listener1: $e');
  });

  final sub2 = controller.stream.listen((e) {
    results2.add(e);
    print('  listener2: $e');
  });

  controller.add('hello');
  controller.add('world');

  // Late listener: misses past events (broadcast streams don't buffer)
  final results3 = <String>[];
  final sub3 = controller.stream.listen((e) {
    results3.add(e);
    print('  listener3: $e');
  });

  controller.add('late');
  await controller.close();

  await Future.wait([sub1.asFuture(), sub2.asFuture(), sub3.asFuture()]);

  print('  listener1 total: $results1');
  print('  listener2 total: $results2');
  print('  listener3 total: $results3 (missed hello & world)');
}

// ─── 5. Controller as a pipe / bridge ────────────────────────────────────────

/// Wraps an existing stream, logging every event that passes through.
Stream<T> logged<T>(Stream<T> source, String label) {
  final controller = StreamController<T>();

  source.listen(
    (event) {
      print('  [$label] data: $event');
      controller.add(event);
    },
    onError: (Object e, StackTrace st) {
      print('  [$label] error: $e');
      controller.addError(e, st);
    },
    onDone: () {
      print('  [$label] done');
      controller.close();
    },
  );

  return controller.stream;
}

Future<void> bridgeDemo() async {
  header('5. Controller as a bridge / decorator');

  final source = Stream.fromIterable([100, 200, 300]);
  final loggedStream = logged(source, 'myStream');

  final result = await loggedStream.toList();
  print('  final list: $result');
}

// ─── 6. addStream — piping another stream into a controller ──────────────────

Future<void> addStreamDemo() async {
  header('6. addStream: pipe a whole stream into a controller');

  final controller = StreamController<int>();
  final source = Stream.fromIterable([7, 8, 9]);

  // addStream returns a Future that completes when the source is drained
  final pipeCompleter = controller.addStream(source);
  controller.add(10); // ERROR: you can't add while addStream is running

  // Correct approach: await the pipe first, then add more
  final controller2 = StreamController<int>();
  final items = <int>[];
  controller2.stream.listen(items.add);

  await controller2.addStream(Stream.fromIterable([1, 2, 3]));
  controller2.add(99);
  await controller2.close();

  // Give the listener a tick to drain
  await Future.delayed(Duration.zero);
  print('  items: $items');

  // Clean up the first controller
  await controller.close();
  await pipeCompleter;
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 6: StreamController ===');

  await basicControllerDemo();
  await callbacksDemo();
  await stateCheckDemo();
  await broadcastControllerDemo();
  await bridgeDemo();
  await addStreamDemo();

  print('''

=== End of Lesson 6 ===

Key takeaways:
  • StreamController gives you a sink (write) + stream (read) pair
  • Single-subscription: only one listener at a time (default)
  • Broadcast: many simultaneous listeners; no buffering for latecomers
  • Callbacks: onListen, onPause, onResume, onCancel for backpressure
  • State flags: hasListener, isPaused, isClosed
  • addStream() pipes an existing stream into the controller
  • Always call controller.close() when you are done pushing events

Next: Lesson 7 — Error handling in streams
''');
}
