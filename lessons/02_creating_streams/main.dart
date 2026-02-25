/// Lesson 2: Creating Streams
///
/// There are three main ways to create a stream:
///   1. Factory constructors  (Stream.fromIterable, Stream.periodic, …)
///   2. Async generators      (async* / yield)
///   3. StreamController      (push data imperatively)
///
/// We cover (1) here. Async generators are Lesson 5,
/// and StreamController is Lesson 6.

import 'dart:async';

// ─── helpers ────────────────────────────────────────────────────────────────

void header(String title) {
  print('\n--- $title ---');
}

// ─── factory constructor demos ───────────────────────────────────────────────

/// Stream.fromIterable wraps any Iterable synchronously.
Future<void> fromIterableDemo() async {
  header('Stream.fromIterable');
  final stream = Stream.fromIterable(['apple', 'banana', 'cherry']);
  await for (final fruit in stream) {
    print('  fruit: $fruit');
  }
}

/// Stream.fromFuture emits exactly ONE event when the Future completes.
Future<void> fromFutureDemo() async {
  header('Stream.fromFuture');
  final future = Future.delayed(
    Duration(milliseconds: 100),
    () => 'fetched data',
  );
  final stream = Stream.fromFuture(future);
  await for (final value in stream) {
    print('  value: $value');
  }
}

/// Stream.fromFutures emits one event per future, in completion order.
Future<void> fromFuturesDemo() async {
  header('Stream.fromFutures');
  final stream = Stream.fromFutures([
    Future.delayed(Duration(milliseconds: 200), () => 'slow'),
    Future.delayed(Duration(milliseconds: 50), () => 'fast'),
    Future.value('instant'),
  ]);
  await for (final v in stream) {
    print('  arrived: $v');
  }
}

/// Stream.periodic fires every [period], passing the invocation count.
Future<void> periodicDemo() async {
  header('Stream.periodic (5 ticks, 150 ms apart)');
  final stream = Stream.periodic(
    Duration(milliseconds: 150),
    (i) => 'tick ${i + 1}',
  ).take(5);

  await for (final tick in stream) {
    print('  $tick');
  }
}

/// Stream.value is shorthand for a single-event stream.
Future<void> valueDemo() async {
  header('Stream.value');
  await for (final v in Stream.value(99)) {
    print('  single value: $v');
  }
}

/// Stream.error emits one error event then closes.
Future<void> errorDemo() async {
  header('Stream.error');
  final stream = Stream<int>.error(
    StateError('something went wrong'),
  );

  try {
    await for (final _ in stream) {}
  } on StateError catch (e) {
    print('  caught error: ${e.message}');
  }
}

/// Stream.multi lets you push events to multiple independent listeners.
/// Each new listener gets its own fresh invocation of the callback.
Future<void> multiDemo() async {
  header('Stream.multi (each listener is independent)');

  // A counter that starts at a different value per listener
  var seed = 0;
  final stream = Stream<int>.multi((controller) {
    final start = seed++ * 10;
    for (var i = start; i < start + 3; i++) {
      controller.add(i);
    }
    controller.close();
  });

  // First listener
  final list1 = await stream.toList();
  // Second listener (gets its own fresh invocation)
  final list2 = await stream.toList();

  print('  listener 1: $list1');
  print('  listener 2: $list2');
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 2: Creating Streams ===');

  await fromIterableDemo();
  await fromFutureDemo();
  await fromFuturesDemo();
  await periodicDemo();
  await valueDemo();
  await errorDemo();
  await multiDemo();

  print('''

=== End of Lesson 2 ===

Key takeaways:
  • Stream.fromIterable  — wrap a synchronous list/Iterable
  • Stream.fromFuture    — one event from a Future
  • Stream.fromFutures   — one event per Future, in completion order
  • Stream.periodic      — repeating timer-driven events
  • Stream.value         — exactly one data event
  • Stream.error         — exactly one error event
  • Stream.multi         — re-runnable stream (one callback per listener)

Next: Lesson 3 — Listening to streams (listen, onData, onError, onDone)
''');
}
