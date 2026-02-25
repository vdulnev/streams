/// Lesson 3: Listening to Streams
///
/// Two styles of consuming a stream:
///   A. await for   — declarative, works inside async functions
///   B. .listen()   — imperative, gives you a StreamSubscription
///
/// StreamSubscription lets you:
///   • pause / resume the stream
///   • cancel early
///   • attach onData / onError / onDone callbacks
///
/// A single-subscription stream can only be listened to ONCE.
/// (Broadcast streams allow multiple listeners — see Lesson 6.)

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── A. await for ────────────────────────────────────────────────────────────

Future<void> awaitForDemo() async {
  header('A. await for (simplest approach)');

  final stream = Stream.fromIterable([10, 20, 30, 40, 50]);

  await for (final value in stream) {
    print('  value: $value');
    if (value == 30) {
      print('  breaking early at 30');
      break; // automatically cancels the subscription
    }
  }
}

// ─── B. .listen() with all callbacks ─────────────────────────────────────────

Future<void> listenWithCallbacksDemo() async {
  header('B. listen(onData, onError, onDone)');

  final completer = Completer<void>();

  Stream.fromIterable([1, 2, 3]).listen(
    (data) => print('  data: $data'),
    onError: (Object e) => print('  error: $e'),
    onDone: () {
      print('  stream closed');
      completer.complete();
    },
    cancelOnError: false, // keep going after errors (default: false)
  );

  await completer.future;
}

// ─── C. StreamSubscription — pause & resume ───────────────────────────────────

Future<void> pauseResumeDemo() async {
  header('C. StreamSubscription — pause and resume');

  final stream = Stream.periodic(
    Duration(milliseconds: 100),
    (i) => i,
  ).take(6);

  late StreamSubscription<int> sub;
  final completer = Completer<void>();

  sub = stream.listen(
    (tick) async {
      print('  tick: $tick');

      if (tick == 2) {
        print('  >> pausing for 300 ms …');
        sub.pause(Future.delayed(Duration(milliseconds: 300)));
        // After the Future completes the subscription auto-resumes
      }
    },
    onDone: () {
      print('  done');
      completer.complete();
    },
  );

  await completer.future;
}

// ─── D. cancel early ─────────────────────────────────────────────────────────

Future<void> cancelDemo() async {
  header('D. cancel() a subscription early');

  final stream = Stream.periodic(
    Duration(milliseconds: 80),
    (i) => i,
  );

  int received = 0;
  late StreamSubscription<int> sub;
  final completer = Completer<void>();

  sub = stream.listen((tick) async {
    print('  tick: $tick');
    received++;
    if (received >= 3) {
      await sub.cancel();
      print('  subscription cancelled after $received events');
      completer.complete();
    }
  });

  await completer.future;
}

// ─── E. onError inside listen ────────────────────────────────────────────────

Future<void> onErrorDemo() async {
  header('E. Handling errors inside listen()');

  // Build a stream that emits 1, then throws, then emits 3
  final controller = StreamController<int>();
  controller.add(1);
  controller.addError(FormatException('bad data'));
  controller.add(3);
  controller.close();

  final completer = Completer<void>();

  controller.stream.listen(
    (v) => print('  data: $v'),
    onError: (Object e) => print('  error caught: $e'),
    onDone: () {
      print('  done');
      completer.complete();
    },
    cancelOnError: false,
  );

  await completer.future;
}

// ─── F. asBroadcastStream preview ────────────────────────────────────────────

Future<void> broadcastPreviewDemo() async {
  header('F. Preview: multiple listeners via asBroadcastStream');

  // Without asBroadcastStream a second listen() would throw.
  final broadcast = Stream.fromIterable([7, 8, 9]).asBroadcastStream();

  // Two concurrent listeners
  final f1 = broadcast.toList();
  final f2 = broadcast.toList();

  final results = await Future.wait([f1, f2]);
  print('  listener 1: ${results[0]}');
  print('  listener 2: ${results[1]}');
  // Note: both listeners see ALL events only if they subscribe before
  // the stream starts emitting. Broadcast streams don't buffer.
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 3: Listening to Streams ===');

  await awaitForDemo();
  await listenWithCallbacksDemo();
  await pauseResumeDemo();
  await cancelDemo();
  await onErrorDemo();
  await broadcastPreviewDemo();

  print('''

=== End of Lesson 3 ===

Key takeaways:
  • await for   — cleanest syntax; break cancels automatically
  • .listen()   — returns a StreamSubscription for manual control
  • sub.pause() — buffers upstream events; sub.resume() drains them
  • sub.cancel()— unsubscribes and frees resources (always await it)
  • cancelOnError: false — keeps the stream alive after an error
  • onDone      — fires once after the stream closes normally

Next: Lesson 4 — Transforming streams (map, where, take, skip, …)
''');
}
