/// Lesson 4: Transforming Streams
///
/// Streams have a rich set of transformation methods — similar to
/// Iterable methods, but async. They all return a new Stream.
///
/// Covered here:
///   map       — transform each event
///   where     — filter events
///   take / takeWhile
///   skip / skipWhile
///   expand    — one-to-many (flatMap for synchronous children)
///   asyncMap  — map with an async callback
///   asyncExpand — flatMap for stream-producing callbacks
///   distinct  — deduplicate consecutive equal events
///   debounce  — drop events too close together (manual implementation)
///   transform — use a custom StreamTransformer

import 'dart:async';

void header(String title) => print('\n--- $title ---');

// ─── map ─────────────────────────────────────────────────────────────────────

Future<void> mapDemo() async {
  header('map: double every number');
  final stream = Stream.fromIterable([1, 2, 3, 4, 5]).map((n) => n * 2);

  await for (final v in stream) {
    print('  $v');
  }
}

// ─── where ───────────────────────────────────────────────────────────────────

Future<void> whereDemo() async {
  header('where: keep only even numbers');
  final stream = Stream.fromIterable(List.generate(10, (i) => i + 1))
      .where((n) => n.isEven);

  await for (final v in stream) {
    print('  $v');
  }
}

// ─── take / takeWhile ────────────────────────────────────────────────────────

Future<void> takeDemo() async {
  header('take(3): stop after 3 events');
  final stream = Stream.fromIterable([10, 20, 30, 40, 50]).take(3);
  await for (final v in stream) print('  $v');

  header('takeWhile: stop when value >= 40');
  final stream2 =
      Stream.fromIterable([10, 20, 30, 40, 50]).takeWhile((n) => n < 40);
  await for (final v in stream2) print('  $v');
}

// ─── skip / skipWhile ────────────────────────────────────────────────────────

Future<void> skipDemo() async {
  header('skip(2): discard first 2 events');
  final stream = Stream.fromIterable([10, 20, 30, 40, 50]).skip(2);
  await for (final v in stream) print('  $v');

  header('skipWhile: discard while value < 30');
  final stream2 =
      Stream.fromIterable([10, 20, 30, 40, 50]).skipWhile((n) => n < 30);
  await for (final v in stream2) print('  $v');
}

// ─── expand (synchronous flatMap) ────────────────────────────────────────────

Future<void> expandDemo() async {
  header('expand: one word → its characters');
  final stream =
      Stream.fromIterable(['hi', 'bye']).expand((word) => word.split(''));
  await for (final ch in stream) print('  "$ch"');
}

// ─── asyncMap ────────────────────────────────────────────────────────────────

Future<String> fakeApiCall(int id) async {
  await Future.delayed(Duration(milliseconds: 50));
  return 'user_$id';
}

Future<void> asyncMapDemo() async {
  header('asyncMap: fetch user for each id (sequential)');
  final stream = Stream.fromIterable([1, 2, 3]).asyncMap(fakeApiCall);
  await for (final user in stream) {
    print('  fetched: $user');
  }
}

// ─── asyncExpand (async flatMap) ─────────────────────────────────────────────

Stream<String> relatedUsers(String user) async* {
  yield '${user}_friend_a';
  await Future.delayed(Duration(milliseconds: 30));
  yield '${user}_friend_b';
}

Future<void> asyncExpandDemo() async {
  header('asyncExpand: expand each user into related users');
  final stream =
      Stream.fromIterable(['alice', 'bob']).asyncExpand(relatedUsers);
  await for (final u in stream) {
    print('  $u');
  }
}

// ─── distinct ────────────────────────────────────────────────────────────────

Future<void> distinctDemo() async {
  header('distinct: remove consecutive duplicates');
  final stream = Stream.fromIterable([1, 1, 2, 2, 2, 3, 1, 1]).distinct();
  await for (final v in stream) print('  $v');
}

// ─── chaining multiple transforms ────────────────────────────────────────────

Future<void> chainingDemo() async {
  header('Chaining: numbers → square → keep >50 → take 4');
  final stream = Stream.fromIterable(List.generate(20, (i) => i + 1))
      .map((n) => n * n) // square
      .where((n) => n > 50) // keep large ones
      .take(4); // only first 4

  await for (final v in stream) print('  $v');
}

// ─── custom StreamTransformer ─────────────────────────────────────────────────

/// A reusable transformer that multiplies every event by [factor].
StreamTransformer<int, String> multiplyBy(int factor) {
  return StreamTransformer.fromHandlers(
    handleData: (data, sink) => sink.add('${data * factor}'),
    handleError: (error, stack, sink) => sink.add('Error: $error'),
    handleDone: (sink) => sink.close(),
  );
}

Future<void> transformerDemo() async {
  header('Custom StreamTransformer: ×3');
  final stream = Stream.fromIterable([1, 2, 3, 4]).map((item) {
    if (item == 3) {
      throw Exception('error');
    } else {
      return item;
    }
  }).transform(multiplyBy(3));
  await for (final v in stream) print('  $v');
}

// ─── entry point ─────────────────────────────────────────────────────────────

void main() async {
  print('=== Lesson 4: Transforming Streams ===');

  await mapDemo();
  await whereDemo();
  await takeDemo();
  await skipDemo();
  await expandDemo();
  await asyncMapDemo();
  await asyncExpandDemo();
  await distinctDemo();
  await chainingDemo();
  await transformerDemo();

  print('''

=== End of Lesson 4 ===

Key takeaways:
  Transformation    │ Purpose
  ──────────────────┼──────────────────────────────────────────
  map               │ 1-to-1 sync transform
  where             │ filter (keep matching events)
  take / takeWhile  │ limit by count or predicate
  skip / skipWhile  │ discard leading events
  expand            │ 1-to-many sync transform
  asyncMap          │ 1-to-1 async transform (sequential)
  asyncExpand       │ 1-to-many async (stream-producing)
  distinct          │ drop consecutive duplicates
  transform         │ plug in a custom StreamTransformer

  • Transforms are lazy — they only run when someone listens.
  • Chain transforms freely; each returns a new Stream.

Next: Lesson 5 — Async generators (async*, yield, await for)
''');
}
