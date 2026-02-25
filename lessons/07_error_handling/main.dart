/// Lesson 7: Error Handling in Streams
///
/// Streams have a first-class error channel — separate from data events.
/// Errors travel down the stream pipeline just like data events do.
///
/// Strategies covered:
///   1. try/catch inside await for
///   2. onError callback in listen()
///   3. handleError() transform
///   4. onErrorReturn / onErrorReturnWith (manual pattern)
///   5. where to re-throw vs swallow
///   6. cancelOnError behaviour
///   7. Zone-level error handling (last resort)

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── Helper: a stream that mixes data and errors ──────────────────────────────

Stream<int> mixedStream() async* {
  yield 1;
  yield 2;
  throw FormatException('bad data at position 3');
  // ignore: dead_code
  yield 4; // never reached
  yield 5;
}

Stream<int> multiErrorStream() {
  final c = StreamController<int>();
  c.add(10);
  c.addError(ArgumentError('wrong arg'));
  c.add(20);
  c.addError(StateError('bad state'));
  c.add(30);
  c.close();
  return c.stream;
}

// ─── 1. try/catch inside await for ───────────────────────────────────────────

Future<void> tryCatchDemo() async {
  header('1. try/catch inside await for');
  // IMPORTANT: a single error cancels the stream!
  // The try/catch catches the error, but iteration stops.
  try {
    await for (final v in mixedStream()) {
      print('  data: $v');
    }
  } on FormatException catch (e) {
    print('  caught FormatException: ${e.message}');
  }
  print('  (stream iteration ended after error)');
}

// ─── 2. onError in listen() with cancelOnError:false ──────────────────────────

Future<void> onErrorListenDemo() async {
  header('2. onError in listen(), cancelOnError: false');

  final completer = Completer<void>();
  multiErrorStream().listen(
    (v) => print('  data: $v'),
    onError: (Object e) => print('  error handled: ${e.runtimeType} — $e'),
    onDone: () {
      print('  done');
      completer.complete();
    },
    cancelOnError: false, // keep going after each error
  );
  await completer.future;
}

// ─── 3. handleError() transform ──────────────────────────────────────────────

Future<void> handleErrorDemo() async {
  header('3. handleError() transform (inline)');

  // handleError intercepts matching errors; non-matching ones propagate
  final stream = multiErrorStream()
      .handleError(
        (Object e) => print('  [handleError] caught: $e'),
        test: (e) => e is ArgumentError, // only catch ArgumentError
      );

  try {
    await for (final v in stream) {
      print('  data: $v');
    }
  } on StateError catch (e) {
    print('  StateError not handled by handleError, caught here: $e');
  }
}

// ─── 4. onErrorReturn pattern — replace error with a fallback value ───────────

extension StreamErrorReturn<T> on Stream<T> {
  /// Replace any error with [fallback] and continue the stream.
  Stream<T> onErrorReturn(T fallback) {
    return transform(
      StreamTransformer.fromHandlers(
        handleData: (data, sink) => sink.add(data),
        handleError: (e, st, sink) {
          print('  [onErrorReturn] swapped error "$e" → $fallback');
          sink.add(fallback);
        },
        handleDone: (sink) => sink.close(),
      ),
    );
  }
}

Future<void> onErrorReturnDemo() async {
  header('4. onErrorReturn: replace errors with a fallback value');
  final stream = multiErrorStream().onErrorReturn(-1);
  await for (final v in stream) {
    print('  value: $v');
  }
}

// ─── 5. Retry pattern ────────────────────────────────────────────────────────

int _attempts = 0;

/// Simulates a flaky async operation that fails the first two times.
Future<String> flakeyFetch() async {
  _attempts++;
  if (_attempts < 3) throw Exception('network error (attempt $_attempts)');
  return 'success after $_attempts attempts';
}

/// Retry a Future-based operation up to [maxAttempts] times.
Stream<T> retry<T>(Future<T> Function() fn, {int maxAttempts = 3}) async* {
  for (var i = 0; i < maxAttempts; i++) {
    try {
      yield await fn();
      return; // success — close the stream
    } catch (e) {
      if (i == maxAttempts - 1) rethrow;
      print('  retry ${i + 1}: $e');
    }
  }
}

Future<void> retryDemo() async {
  header('5. Retry pattern');
  await for (final result in retry(flakeyFetch, maxAttempts: 5)) {
    print('  result: $result');
  }
}

// ─── 6. cancelOnError: true (default for await for) ──────────────────────────

Future<void> cancelOnErrorDemo() async {
  header('6. cancelOnError: true — cancel stream on first error');

  var received = 0;
  final completer = Completer<void>();
  multiErrorStream().listen(
    (v) {
      received++;
      print('  data: $v');
    },
    onError: (Object e) {
      print('  error (stream cancelled): $e');
      completer.complete();
    },
    cancelOnError: true, // stop at first error
  );
  await completer.future;
  print('  total events received before cancel: $received');
}

// ─── 7. Zone error handler as a last resort ───────────────────────────────────

Future<void> zoneErrorDemo() async {
  header('7. Zone error handler (catches unhandled stream errors)');

  final completer = Completer<void>();

  runZonedGuarded(
    () async {
      // This stream has no error handler — the error escapes to the Zone
      final c = StreamController<int>();
      c.add(42);
      c.addError(Exception('unhandled stream error'));
      c.close();

      // Listen without onError — error propagates to zone
      await for (final v in c.stream) {
        print('  data: $v');
      }
      completer.complete();
    },
    (error, stack) {
      print('  zone caught: $error');
      if (!completer.isCompleted) completer.complete();
    },
  );

  await completer.future;
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 7: Error Handling in Streams ===');

  await tryCatchDemo();
  await onErrorListenDemo();
  await handleErrorDemo();
  await onErrorReturnDemo();
  await retryDemo();
  await cancelOnErrorDemo();
  await zoneErrorDemo();

  print('''

=== End of Lesson 7 ===

Key takeaways:
  Strategy              │ Behaviour
  ──────────────────────┼────────────────────────────────────────────────
  try/catch (await for) │ catches, but iteration stops after error
  onError in listen()   │ per-event; use cancelOnError:false to continue
  handleError()         │ inline filter; optional test: predicate
  Custom transformer    │ most flexible; replace/swallow/transform errors
  Retry pattern         │ re-subscribe (async*) on failure
  runZonedGuarded       │ last-resort catch-all for unhandled errors

  • Prefer handleError() or onError over try/catch when you want to
    keep the stream alive after an error.
  • Always decide: should an error cancel the stream or be handled?
  • Unhandled stream errors in a Zone can crash the isolate.

Next: Lesson 8 — Advanced patterns (debounce, throttle, merging streams)
''');
}
