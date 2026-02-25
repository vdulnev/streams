# Dart Streams — Complete Learning Course

A hands-on course teaching Dart Streams from scratch to real-world patterns.
Each lesson is a self-contained Dart program you can read and run.

## Prerequisites

- Dart SDK ≥ 3.0  (`dart --version`)
- Basic knowledge of `async`/`await` and `Future`

## Running a lesson

```bash
dart run lessons/01_introduction/main.dart
```

---

## Course Map

| # | Lesson | Key concepts |
|---|--------|-------------|
| 01 | [Introduction](lessons/01_introduction/main.dart) | What streams are, `await for`, `Stream.fromIterable`, `Stream.periodic` |
| 02 | [Creating Streams](lessons/02_creating_streams/main.dart) | `fromIterable`, `fromFuture`, `fromFutures`, `periodic`, `value`, `error`, `multi` |
| 03 | [Listening](lessons/03_listening/main.dart) | `listen()`, `StreamSubscription`, `pause`, `resume`, `cancel`, `onDone` |
| 04 | [Transformations](lessons/04_transformations/main.dart) | `map`, `where`, `take`, `skip`, `expand`, `asyncMap`, `asyncExpand`, `distinct`, `StreamTransformer` |
| 05 | [Async Generators](lessons/05_async_generators/main.dart) | `async*`, `yield`, `yield*`, `sync*`, recursive generators |
| 06 | [StreamController](lessons/06_stream_controller/main.dart) | `StreamController`, broadcast vs single-subscription, `addStream` |
| 07 | [Error Handling](lessons/07_error_handling/main.dart) | `handleError`, `onError`, `cancelOnError`, retry pattern, `runZonedGuarded` |
| 08 | [Advanced Patterns](lessons/08_advanced_patterns/main.dart) | merge, zip, combineLatest, debounce, throttle, buffer, switchMap, scan |
| 09 | [Real World](lessons/09_real_world/main.dart) | Live chat system combining all concepts |

---

## Mental Model

```
Producer ──► Stream<T> ──► Transforms ──► Listener
  (data source)   (pipe)    (map/where/…)  (await for / listen)
```

A `Stream<T>` is the **async** equivalent of `Iterable<T>`:

| Sync | Async |
|------|-------|
| `Iterable<T>` | `Stream<T>` |
| `for` loop | `await for` loop |
| `sync*` / `yield` | `async*` / `yield` |
| `List`, `Set` | `StreamController` |

---

## Quick Reference

### Creating

```dart
Stream.fromIterable([1, 2, 3])
Stream.periodic(Duration(seconds: 1), (i) => i)
Stream.value(42)
Stream.error(Exception('oops'))

// async generator
Stream<int> counter() async* {
  for (var i = 0; ; i++) {
    await Future.delayed(Duration(seconds: 1));
    yield i;
  }
}
```

### Listening

```dart
// Style A — declarative
await for (final v in stream) { ... }

// Style B — imperative
final sub = stream.listen(
  (v)  => print(v),
  onError: (e) => print('error: $e'),
  onDone: () => print('done'),
  cancelOnError: false,
);
await sub.cancel();
```

### Transforming

```dart
stream
  .map((n) => n * 2)
  .where((n) => n > 5)
  .take(10)
  .asyncMap(fetchFromApi)
  .distinct()
```

### StreamController

```dart
// Single-subscription
final c = StreamController<int>();
c.add(1);
c.addError(Exception());
await c.close();

// Broadcast (multiple listeners)
final bc = StreamController<int>.broadcast();
```

### Error Handling

```dart
stream
  .handleError((e) => print('caught: $e'), test: (e) => e is IOException)
  .listen(..., cancelOnError: false);
```

---

## Learning Path

```
Lesson 01 ──► Lesson 02 ──► Lesson 03
                                │
                                ▼
              Lesson 05 ◄── Lesson 04
                 │
                 ▼
              Lesson 06 ──► Lesson 07
                                │
                                ▼
                            Lesson 08
                                │
                                ▼
                            Lesson 09  ← capstone project
```
