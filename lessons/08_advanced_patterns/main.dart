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

import 'package:rxdart/rxdart.dart';

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
  Stream<T> debounceManual(Duration duration) {
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
      .debounceManual(Duration(milliseconds: 150))
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
  Stream<T> throttleManual(Duration duration) {
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

  await for (final v in source.throttleManual(Duration(milliseconds: 200))) {
    results.add(v);
    print('  throttled: $v');
  }
  print('  total emitted: ${results.length} out of 10');
}

// ─── 6. buffer (batch events) ────────────────────────────────────────────────

extension BufferExtension<T> on Stream<T> {
  /// Collect events into lists of [size].
  Stream<List<T>> bufferManual(int size) async* {
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
  await for (final batch in stream.bufferManual(3)) {
    print('  batch: $batch');
  }
}

// ─── 7. switchMap ────────────────────────────────────────────────────────────

extension SwitchMapExtension<T> on Stream<T> {
  /// For each event, start a new inner stream and cancel the previous one.
  Stream<R> switchMapManual<R>(Stream<R> Function(T) mapper) {
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
      .switchMapManual(search)
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
  Stream<S> scanManual<S>(S seed, S Function(S acc, T event) combine) async* {
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
      .scanManual<int>(0, (acc, e) => acc + e);
  await for (final sum in stream) {
    print('  running sum: $sum');
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// PART 2 — RxDart equivalents
//
// RxDart wraps Dart streams with a rich operator library modelled after
// ReactiveX (RxJS / RxJava / RxSwift). Each demo below mirrors the manual
// implementation above so you can compare the two side-by-side.
//
// Key types:
//   Rx               — static factory methods (merge, zip, combineLatest…)
//   BehaviorSubject  — broadcast StreamController that caches the last value
//   PublishSubject   — standard broadcast StreamController
//   ReplaySubject    — replays all past events to every new subscriber
// ═══════════════════════════════════════════════════════════════════════════

// ─── rx.1. Rx.merge ───────────────────────────────────────────────────────────

Future<void> rxMergeDemo() async {
  header('rx.1. Rx.merge');

  final stream = Rx.merge([
    Stream.periodic(Duration(milliseconds: 100), (_) => 'A').take(3),
    Stream.periodic(Duration(milliseconds: 150), (_) => 'B').take(3),
  ]);

  await for (final e in stream) print('  $e');
}

// ─── rx.2. Rx.zip ────────────────────────────────────────────────────────────

Future<void> rxZipDemo() async {
  header('rx.2. Rx.zip2');

  final stream = Rx.zip2(
    Stream.fromIterable(['A', 'B', 'C', 'D']),
    Stream.fromIterable([1, 2, 3]),
    (l, n) => '$l$n',
  );

  await for (final pair in stream) print('  $pair');
}

// ─── rx.3. Rx.combineLatest ──────────────────────────────────────────────────

Future<void> rxCombineLatestDemo() async {
  header('rx.3. Rx.combineLatest2');

  final stream = Rx.combineLatest2(
    Stream.periodic(Duration(milliseconds: 120), (i) => 20 + i).take(3),
    Stream.periodic(Duration(milliseconds: 80), (i) => 50 + i * 5).take(4),
    (t, h) => 'temp=$t°C hum=$h%',
  );

  await for (final reading in stream) print('  $reading');
}

// ─── rx.4. debounceTime ───────────────────────────────────────────────────────

Future<void> rxDebounceDemo() async {
  header('rx.4. debounceTime');

  final keystrokes = StreamController<String>();
  final results = <String>[];

  final sub = keystrokes.stream
      .debounceTime(Duration(milliseconds: 150))
      .listen((v) {
    results.add(v);
    print('  debounced emit: "$v"');
  });

  for (final char in ['d', 'da', 'dar', 'dart']) {
    keystrokes.add(char);
    await Future.delayed(Duration(milliseconds: 40));
  }
  await Future.delayed(Duration(milliseconds: 300));

  await keystrokes.close();
  await sub.cancel();
  print('  total emitted: ${results.length} (expected 1)');
}

// ─── rx.5. throttleTime ──────────────────────────────────────────────────────

Future<void> rxThrottleDemo() async {
  header('rx.5. throttleTime (leading edge)');

  final source = Stream.periodic(Duration(milliseconds: 60), (i) => i).take(10);
  final results = <int>[];

  await for (final v in source.throttleTime(Duration(milliseconds: 200))) {
    results.add(v);
    print('  throttled: $v');
  }
  print('  total emitted: ${results.length} out of 10');
}

// ─── rx.6. bufferCount ───────────────────────────────────────────────────────

Future<void> rxBufferDemo() async {
  header('rx.6. bufferCount');

  final stream = Stream.fromIterable(List.generate(8, (i) => i + 1))
      .bufferCount(3);

  await for (final batch in stream) print('  batch: $batch');
}

// ─── rx.7. switchMap ─────────────────────────────────────────────────────────

Future<void> rxSwitchMapDemo() async {
  header('rx.7. switchMap (rxdart built-in)');

  final queries = StreamController<String>();
  final results = <String>[];

  // Use a Completer to wait for the result rather than relying on a fixed
  // delay — rxdart's switchMap delivery and sub.cancel() can otherwise race.
  final resultArrived = Completer<void>();

  final sub = queries.stream.switchMap(search).listen((r) {
    results.add(r);
    print('  $r');
    resultArrived.complete();
  });

  queries.add('d');
  await Future.delayed(Duration(milliseconds: 30));
  queries.add('da');
  await Future.delayed(Duration(milliseconds: 30));
  queries.add('dart'); // only this search will complete

  await resultArrived.future; // wait for 'dart' result to arrive
  await sub.cancel();
  await queries.close();
  print('  total results: ${results.length} (expected 1)');
}

// ─── rx.8. scan ──────────────────────────────────────────────────────────────

Future<void> rxScanDemo() async {
  header('rx.8. scan (rxdart — seed is 2nd arg, callback receives index too)');

  // rxdart signature: scan<S>(S Function(S acc, T value, int index), S seed)
  final stream = Stream.fromIterable([1, 2, 3, 4, 5])
      .scan<int>((acc, e, _) => acc + e, 0);

  await for (final sum in stream) print('  running sum: $sum');
}

// ─── rx.9. BehaviorSubject ───────────────────────────────────────────────────

Future<void> behaviorSubjectDemo() async {
  header('rx.9. BehaviorSubject — caches the last value for late subscribers');

  final subject = BehaviorSubject<int>.seeded(0);

  subject.add(1);
  subject.add(2);
  subject.add(3);

  print('  current value: ${subject.value}');

  // A new listener immediately receives the last cached value (3)
  final received = <int>[];
  final sub = subject.listen((v) {
    received.add(v);
    print('  late subscriber got: $v');
  });

  subject.add(4);
  await Future.delayed(Duration.zero);

  await sub.cancel();
  await subject.close();
  print('  received: $received  (3 replayed + 4 live)');
}

// ─── rx.10. PublishSubject ───────────────────────────────────────────────────

Future<void> publishSubjectDemo() async {
  header('rx.10. PublishSubject — broadcast, no caching');

  final subject = PublishSubject<String>();

  final log1 = <String>[];
  final log2 = <String>[];

  final sub1 = subject.listen((v) {
    log1.add(v);
    print('  sub1: $v');
  });

  subject.add('hello');

  // sub2 subscribes AFTER 'hello' — does NOT receive it
  final sub2 = subject.listen((v) {
    log2.add(v);
    print('  sub2: $v');
  });

  subject.add('world');
  await Future.delayed(Duration.zero);

  await sub1.cancel();
  await sub2.cancel();
  await subject.close();

  print('  sub1 got: $log1');
  print('  sub2 got: $log2  (missed "hello")');
}

// ─── rx.11. ReplaySubject ────────────────────────────────────────────────────

Future<void> replaySubjectDemo() async {
  header('rx.11. ReplaySubject — replays all past events to new subscribers');

  final subject = ReplaySubject<int>();

  subject.add(1);
  subject.add(2);
  subject.add(3);

  // Late subscriber receives ALL past events immediately
  final received = <int>[];
  final sub = subject.listen((v) {
    received.add(v);
    print('  replayed: $v');
  });

  subject.add(4); // also received live
  await Future.delayed(Duration.zero);

  await sub.cancel();
  await subject.close();
  print('  total received: $received');
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 8: Advanced Stream Patterns ===');
  print('\n── Part 1: Manual implementations ──');

  await mergeDemo();
  await zipDemo();
  await combineLatestDemo();
  await debounceDemo();
  await throttleDemo();
  await bufferDemo();
  await switchMapDemo();
  await scanDemo();

  print('\n── Part 2: RxDart equivalents ──');

  await rxMergeDemo();
  await rxZipDemo();
  await rxCombineLatestDemo();
  await rxDebounceDemo();
  await rxThrottleDemo();
  await rxBufferDemo();
  await rxSwitchMapDemo();
  await rxScanDemo();
  await behaviorSubjectDemo();
  await publishSubjectDemo();
  await replaySubjectDemo();

  print('''

=== End of Lesson 8 ===

Pattern              │ Manual (this file)        │ RxDart
─────────────────────┼───────────────────────────┼───────────────────────
merge                │ merge() helper fn          │ Rx.merge([...])
zip                  │ zip() helper fn            │ Rx.zip2(a, b, fn)
combineLatest        │ combineLatest() helper fn  │ Rx.combineLatest2(...)
debounce             │ .debounceManual()          │ .debounceTime()
throttle             │ .throttleManual()          │ .throttleTime()
buffer by count      │ .bufferManual(n)           │ .bufferCount(n)
switchMap            │ .switchMapManual()         │ .switchMap()
scan / fold          │ .scanManual(seed, fn)      │ .scan(fn, seed)
broadcast controller │ StreamController.broadcast │ PublishSubject
cached last value    │ (manual BehaviorSubject)   │ BehaviorSubject
replay all events    │ (manual ReplaySubject)     │ ReplaySubject

Next: Lesson 9 — Real-world application: a simulated live chat system
''');
}
