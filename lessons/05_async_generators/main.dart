/// Lesson 5: Async Generators — async* and yield
///
/// An async generator is a function that:
///   • is marked  async*
///   • returns    Stream<T>
///   • uses       yield  to emit one event
///                yield* to forward all events from another stream/iterable
///
/// The body executes lazily: it pauses whenever the listener isn't ready
/// and resumes automatically. It's the cleanest way to build custom streams.

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── 1. Basic counter stream ──────────────────────────────────────────────────

/// Counts from [start] to [end] (inclusive), one event every [delay].
Stream<int> counter({
  required int start,
  required int end,
  Duration delay = const Duration(milliseconds: 100),
}) async* {
  for (var i = start; i <= end; i++) {
    await Future.delayed(delay);
    yield i; // <-- emit one event
  }
  // The stream automatically closes when the function returns
}

Future<void> basicCounterDemo() async {
  header('1. Basic counter (1 to 5)');
  await for (final n in counter(start: 1, end: 5)) {
    print('  $n');
  }
}

// ─── 2. yield* — forward another stream ──────────────────────────────────────

/// Concatenates two counters.
Stream<int> concat(Stream<int> a, Stream<int> b) async* {
  yield* a; // drain all events from stream a first …
  yield* b; // … then all from stream b
}

Future<void> yieldStarDemo() async {
  header('2. yield* — concat two streams');
  final combined = concat(
    counter(start: 1, end: 3, delay: Duration(milliseconds: 50)),
    counter(start: 10, end: 12, delay: Duration(milliseconds: 50)),
  );
  await for (final n in combined) {
    print('  $n');
  }
}

// ─── 3. Fibonacci generator ───────────────────────────────────────────────────

Stream<int> fibonacci(int count) async* {
  int a = 0, b = 1;
  for (var i = 0; i < count; i++) {
    yield a;
    final next = a + b;
    a = b;
    b = next;
  }
}

Future<void> fibonacciDemo() async {
  header('3. Fibonacci sequence (first 10)');
  await for (final n in fibonacci(10)) {
    stdout.write('$n ');
  }
  print();
}

// ─── 4. Infinite stream with cancellation ────────────────────────────────────

/// An infinite stream of random-ish sensor readings.
Stream<double> sensorReadings() async* {
  var value = 20.0;
  while (true) {
    await Future.delayed(Duration(milliseconds: 80));
    value += (value.hashCode % 5) - 2; // fake delta
    yield value;
  }
}

Future<void> infiniteStreamDemo() async {
  header('4. Infinite sensor stream — take 6 readings');
  await for (final reading in sensorReadings().take(6)) {
    print('  temperature: ${reading.toStringAsFixed(1)} °C');
  }
  // .take(6) cancels the infinite stream automatically
}

// ─── 5. Generator with error ──────────────────────────────────────────────────

Stream<int> riskyStream() async* {
  yield 1;
  yield 2;
  throw StateError('oops — something went wrong');
  // The line below is never reached, but Dart won't complain:
  // ignore: dead_code
  yield 3;
}

Future<void> generatorWithErrorDemo() async {
  header('5. Generator that throws (caught with try/catch inside await for)');
  try {
    await for (final v in riskyStream()) {
      print('  got: $v');
    }
  } on StateError catch (e) {
    print('  caught: ${e.message}');
  }
}

// ─── 6. Recursive generator ───────────────────────────────────────────────────

/// Walks a tree depth-first and yields each node's value.
class Node {
  final int value;
  final List<Node> children;
  Node(this.value, [this.children = const []]);
}

Stream<int> walk(Node node) async* {
  yield node.value;
  for (final child in node.children) {
    yield* walk(child); // recurse
  }
}

Future<void> treeWalkDemo() async {
  header('6. Recursive tree walk with yield*');
  //       1
  //      / \
  //     2   3
  //    / \
  //   4   5
  final tree = Node(1, [
    Node(2, [Node(4), Node(5)]),
    Node(3),
  ]);

  await for (final v in walk(tree)) {
    stdout.write('$v ');
  }
  print();
}

// ─── 7. Sync generator vs async generator ────────────────────────────────────

// sync*  → returns Iterable<T>, uses yield, no await
Iterable<int> syncRange(int n) sync* {
  for (var i = 0; i < n; i++) yield i;
}

// async* → returns Stream<T>, uses yield + await
Stream<int> asyncRange(int n) async* {
  for (var i = 0; i < n; i++) {
    await Future.delayed(Duration(milliseconds: 10));
    yield i;
  }
}

Future<void> syncVsAsyncDemo() async {
  header('7. sync* vs async*');
  print('  sync*  (Iterable): ${syncRange(5).toList()}');
  print('  async* (Stream):   ${await asyncRange(5).toList()}');
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// dart:io is not available in some environments; use a shim if needed
final stdout = _StdoutShim();

class _StdoutShim {
  void write(Object v) => _buf.write(v);
  final _buf = StringBuffer();
}

extension on _StdoutShim {
  // ignore: unused_element
  void flush() {
    if (_buf.isNotEmpty) {
      print(_buf.toString());
      _buf.clear();
    }
  }
}

// Patch: replace stdout.write with a print helper for portability
void _write(Object v) => _buf.write(v);
final _buf = StringBuffer();
void _flush() {
  if (_buf.isNotEmpty) {
    print(_buf.toString().trimRight());
    _buf.clear();
  }
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 5: Async Generators ===');

  // Swap stdout.write for buffered printing (no dart:io needed)
  await _runWithBuf();

  print('''

=== End of Lesson 5 ===

Key takeaways:
  • async*  marks a function as an async generator → returns Stream<T>
  • yield   emits one event and suspends until the listener is ready
  • yield*  forwards ALL events from another Stream or Iterable
  • return  (or reaching end of function) closes the stream
  • throw   inside an async* emits an error event, then closes
  • Combine with .take(), .where(), etc. just like any other Stream
  • sync*  is the synchronous sibling → returns Iterable<T>

Next: Lesson 6 — StreamController (push data imperatively)
''');
}

Future<void> _runWithBuf() async {
  // Redefine helpers to use buffered output
  Future<void> run(String title, Future<void> Function() fn) async {
    print('\n--- $title ---');
    await fn();
    _flush();
  }

  await run('1. Basic counter (1 to 5)', () async {
    await for (final n in counter(start: 1, end: 5)) print('  $n');
  });

  await run('2. yield* — concat two streams', () async {
    final combined = concat(
      counter(start: 1, end: 3, delay: Duration(milliseconds: 50)),
      counter(start: 10, end: 12, delay: Duration(milliseconds: 50)),
    );
    await for (final n in combined) print('  $n');
  });

  await run('3. Fibonacci sequence (first 10)', () async {
    final buf = StringBuffer('  ');
    await for (final n in fibonacci(10)) buf.write('$n ');
    print(buf.toString().trimRight());
  });

  await run('4. Infinite sensor stream — take 6 readings', () async {
    await for (final r in sensorReadings().take(6)) {
      print('  temperature: ${r.toStringAsFixed(1)} °C');
    }
  });

  await run(
      '5. Generator that throws (caught with try/catch inside await for)',
      () async {
    try {
      await for (final v in riskyStream()) print('  got: $v');
    } on StateError catch (e) {
      print('  caught: ${e.message}');
    }
  });

  await run('6. Recursive tree walk with yield*', () async {
    final tree = Node(1, [
      Node(2, [Node(4), Node(5)]),
      Node(3),
    ]);
    final buf = StringBuffer('  ');
    await for (final v in walk(tree)) buf.write('$v ');
    print(buf.toString().trimRight());
  });

  await run('7. sync* vs async*', () async {
    print('  sync*  (Iterable): ${syncRange(5).toList()}');
    print('  async* (Stream):   ${await asyncRange(5).toList()}');
  });
}
