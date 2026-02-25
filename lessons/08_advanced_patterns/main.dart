/// Lesson 8: Advanced Stream Patterns
///
/// Real-world reactive code needs patterns beyond basic transforms:
///
///   1. merge        — interleave two streams into one
///   2. zip          — pair events from two streams together
///   3. combineLatest— emit whenever either stream emits (latest pair)
///   4. debounce     — ignore bursts; emit only after a quiet period
///   5. throttle     — emit at most once per time window
///   6. buffer       — batch events into lists
///   7. switchMap    — cancel old inner stream when source emits
///   8. scan         — running fold / accumulator
///   9. window       — emit sub-streams of fixed size

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── 1. merge ────────────────────────────────────────────────────────────────

/// Interleave events from two streams in arrival order.
Stream<T> merge<T>(Stream<T> a, Stream<T> b) {
  final controller = StreamController<T>.broadcast();

  var openStreams = 2;
  void onDone() {
    openStreams--;
    if (openStreams == 0) controller.close();
  }

  a.listen(controller.add, onError: controller.addError, onDone: onDone);
  b.listen(controller.add, onError: controller.addError, onDone: onDone);

  return controller.stream;
}

Future<void> mergeDemo() async {
  header('1. merge: interleave two periodic streams');

  final stream = merge(
    Stream.periodic(Duration(milliseconds: 100), (_) => 'A').take(3),
    Stream.periodic(Duration(milliseconds: 150), (_) => 'B').take(3),
  );

  final events = <String>[];
  await for (final e in stream) {
    events.add(e);
    print('  $e');
  }
}

// ─── 2. zip ──────────────────────────────────────────────────────────────────

/// Pair the nth event of [a] with the nth event of [b].
Stream<R> zip<A, B, R>(
  Stream<A> a,
  Stream<B> b,
  R Function(A, B) combine,
) async* {
  final iterA = StreamIterator(a);
  final iterB = StreamIterator(b);

  try {
    while (await iterA.moveNext() && await iterB.moveNext()) {
      yield combine(iterA.current, iterB.current);
    }
  } finally {
    await iterA.cancel();
    await iterB.cancel();
  }
}

Future<void> zipDemo() async {
  header('2. zip: pair letters with numbers');
  final letters = Stream.fromIterable(['A', 'B', 'C', 'D']);
  final numbers = Stream.fromIterable([1, 2, 3]);

  await for (final pair in zip(letters, numbers, (l, n) => '$l$n')) {
    print('  $pair');
  }
}

// ─── 3. combineLatest ────────────────────────────────────────────────────────

/// Emit the latest value from both streams whenever either emits.
Stream<R> combineLatest<A, B, R>(
  Stream<A> a,
  Stream<B> b,
  R Function(A, B) combine,
) {
  final controller = StreamController<R>.broadcast();
  A? latestA;
  B? latestB;
  bool hasA = false, hasB = false;
  var done = 0;

  void tryEmit() {
    if (hasA && hasB) controller.add(combine(latestA as A, latestB as B));
  }

  a.listen(
    (v) { latestA = v; hasA = true; tryEmit(); },
    onDone: () { if (++done == 2) controller.close(); },
  );
  b.listen(
    (v) { latestB = v; hasB = true; tryEmit(); },
    onDone: () { if (++done == 2) controller.close(); },
  );

  return controller.stream;
}

Future<void> combineLatestDemo() async {
  header('3. combineLatest: combine temperature + humidity');

  final temps = Stream.periodic(
    Duration(milliseconds: 120), (i) => 20 + i,
  ).take(3);

  final humidities = Stream.periodic(
    Duration(milliseconds: 80), (i) => 50 + i * 5,
  ).take(4);

  await for (final reading in combineLatest(
    temps, humidities, (t, h) => 'temp=$t°C hum=$h%',
  )) {
    print('  $reading');
  }
}

// ─── 4. debounce ─────────────────────────────────────────────────────────────

extension DebounceExtension<T> on Stream<T> {
  /// Emit only if [duration] has elapsed since the last event.
  Stream<T> debounce(Duration duration) {
    Timer? timer;
    final controller = StreamController<T>.broadcast();

    listen(
      (event) {
        timer?.cancel();
        timer = Timer(duration, () => controller.add(event));
      },
      onDone: () {
        timer?.cancel();
        controller.close();
      },
    );

    return controller.stream;
  }
}

Future<void> debounceDemo() async {
  header('4. debounce: only emit after 150 ms quiet period');

  // Simulate rapid keystrokes
  final keystrokes = StreamController<String>();
  final results = <String>[];

  final sub = keystrokes.stream
      .debounce(Duration(milliseconds: 150))
      .listen((v) {
    results.add(v);
    print('  debounced emit: "$v"');
  });

  // Fire rapid events
  for (final char in ['d', 'da', 'dar', 'dart']) {
    keystrokes.add(char);
    await Future.delayed(Duration(milliseconds: 40));
  }

  // Wait for debounce timeout
  await Future.delayed(Duration(milliseconds: 300));

  await keystrokes.close();
  await sub.cancel();
  print('  total emitted: ${results.length} (expected 1)');
}

// ─── 5. throttle ─────────────────────────────────────────────────────────────

extension ThrottleExtension<T> on Stream<T> {
  /// Allow at most one event per [duration] window (leading edge).
  Stream<T> throttle(Duration duration) {
    bool open = true;
    final controller = StreamController<T>.broadcast();

    listen(
      (event) {
        if (open) {
          open = false;
          controller.add(event);
          Future.delayed(duration, () => open = true);
        }
      },
      onDone: controller.close,
    );

    return controller.stream;
  }
}

Future<void> throttleDemo() async {
  header('5. throttle: at most one event per 200 ms');

  final source = Stream.periodic(Duration(milliseconds: 60), (i) => i).take(10);
  final results = <int>[];

  await for (final v in source.throttle(Duration(milliseconds: 200))) {
    results.add(v);
    print('  throttled: $v');
  }
  print('  total emitted: ${results.length} out of 10');
}

// ─── 6. buffer (batch events) ────────────────────────────────────────────────

extension BufferExtension<T> on Stream<T> {
  /// Collect events into lists of [size].
  Stream<List<T>> buffer(int size) async* {
    final batch = <T>[];
    await for (final event in this) {
      batch.add(event);
      if (batch.length == size) {
        yield List<T>.from(batch);
        batch.clear();
      }
    }
    if (batch.isNotEmpty) yield batch; // flush remainder
  }
}

Future<void> bufferDemo() async {
  header('6. buffer: group events into batches of 3');
  final stream = Stream.fromIterable(List.generate(8, (i) => i + 1));
  await for (final batch in stream.buffer(3)) {
    print('  batch: $batch');
  }
}

// ─── 7. switchMap ────────────────────────────────────────────────────────────

extension SwitchMapExtension<T> on Stream<T> {
  /// For each event, start a new inner stream and cancel the previous one.
  Stream<R> switchMap<R>(Stream<R> Function(T) mapper) {
    final controller = StreamController<R>.broadcast();
    StreamSubscription<R>? innerSub;

    listen(
      (event) {
        innerSub?.cancel();
        innerSub = mapper(event).listen(
          controller.add,
          onError: controller.addError,
        );
      },
      onDone: () async {
        await innerSub?.asFuture();
        controller.close();
      },
    );

    return controller.stream;
  }
}

Stream<String> search(String query) async* {
  await Future.delayed(Duration(milliseconds: 100));
  yield 'results for: "$query"';
}

Future<void> switchMapDemo() async {
  header('7. switchMap: cancel previous search on new query');

  final queries = StreamController<String>();
  final results = <String>[];

  final sub = queries.stream
      .switchMap(search)
      .listen((r) {
    results.add(r);
    print('  $r');
  });

  queries.add('d');
  await Future.delayed(Duration(milliseconds: 30));
  queries.add('da');
  await Future.delayed(Duration(milliseconds: 30));
  queries.add('dart'); // only this will complete (others cancelled)
  await Future.delayed(Duration(milliseconds: 200));

  await queries.close();
  await sub.cancel();
  print('  total results: ${results.length} (expected 1)');
}

// ─── 8. scan (running accumulator) ───────────────────────────────────────────

extension ScanExtension<T> on Stream<T> {
  Stream<S> scan<S>(S seed, S Function(S acc, T event) combine) async* {
    var acc = seed;
    await for (final event in this) {
      acc = combine(acc, event);
      yield acc;
    }
  }
}

Future<void> scanDemo() async {
  header('8. scan: running sum');
  final stream = Stream.fromIterable([1, 2, 3, 4, 5])
      .scan<int>(0, (acc, e) => acc + e);
  await for (final sum in stream) {
    print('  running sum: $sum');
  }
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 8: Advanced Stream Patterns ===');

  await mergeDemo();
  await zipDemo();
  await combineLatestDemo();
  await debounceDemo();
  await throttleDemo();
  await bufferDemo();
  await switchMapDemo();
  await scanDemo();

  print('''

=== End of Lesson 8 ===

Pattern         │ Use case
────────────────┼────────────────────────────────────────────────────
merge           │ interleave two sources; order = arrival order
zip             │ pair nth events; stops when shorter stream ends
combineLatest   │ always-fresh pair; great for form validation
debounce        │ search boxes, resize handlers — ignore bursts
throttle        │ rate-limit button clicks, scroll events
buffer          │ batch DB inserts, paginated UI
switchMap       │ type-ahead search — only latest query matters
scan            │ running totals, undo stacks, state machines

Next: Lesson 9 — Real-world application: a simulated live chat system
''');
}
