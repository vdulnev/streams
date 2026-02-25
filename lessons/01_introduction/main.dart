/// Lesson 1: Introduction to Streams
///
/// A Stream is a sequence of asynchronous events.
/// Think of it like a pipe: data flows through it over time.
///
/// Key concepts:
/// - A Stream emits 0 or more data events
/// - A Stream can emit an error event
/// - A Stream ends with a done event
///
/// Analogy: A river — water (data) flows continuously.
/// You stand on the bank and react to whatever flows past.

import 'dart:async';

void main() async {
  print('=== Lesson 1: Introduction to Streams ===\n');

  // --- 1. The simplest stream: from a list ---
  print('--- 1. Stream from a list ---');
  final Stream<int> numbers = Stream.fromIterable([1, 2, 3, 4, 5]);

  // Listen to each event with await for (clean, readable)
  await for (final n in numbers) {
    print('  received: $n');
  }
  print('  stream done!\n');

  // --- 2. A stream of futures ---
  print('--- 2. Stream from futures ---');
  final Stream<String> messages = Stream.fromFutures([
    Future.value('hello'),
    Future.delayed(Duration(milliseconds: 50), () => 'world'),
    Future.value('!'),
  ]);

  await for (final msg in messages) {
    print('  message: $msg');
  }
  print('');

  // --- 3. Visualising the stream timeline ---
  print('--- 3. A periodic stream (ticks every 200ms, take 4) ---');
  final Stream<int> ticker = Stream.periodic(
    Duration(milliseconds: 200),
    (tick) => tick + 1, // transform the tick count
  ).take(4);

  await for (final tick in ticker) {
    print('  tick #$tick  (time: ${DateTime.now().millisecondsSinceEpoch % 10000}ms)');
  }
  print('');

  // --- 4. A single-value stream ---
  print('--- 4. Stream.value (one event, then done) ---');
  await for (final v in Stream.value(42)) {
    print('  got: $v');
  }
  print('');

  // --- 5. An empty stream ---
  print('--- 5. Stream.empty (zero events, immediately done) ---');
  var count = 0;
  await for (final _ in Stream<int>.empty()) {
    count++;
  }
  print('  events received: $count  (expected 0)\n');

  print('=== End of Lesson 1 ===');
  print('''
Key takeaways:
  • Stream<T> is the async equivalent of Iterable<T>
  • Use "await for" to consume events one at a time
  • Streams have a lifecycle: data events → optional error → done
  • Stream.fromIterable, Stream.value, Stream.empty, Stream.periodic
    are handy factory constructors for quick demos and testing
''');
}
