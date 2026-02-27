/// Lesson 9: Real-World Application — Live Chat System
///
/// This lesson ties together everything you have learned:
///
///   • StreamController (broadcast) — the message bus
///   • async generators — simulated user bots
///   • Stream transforms — filtering, mapping, scan (unread count)
///   • Error handling   — invalid messages rejected gracefully
///   • debounce         — typing indicator
///   • StreamSubscription management — clean shutdown
///
/// Architecture:
///
///   ┌────────────────────────────────────────────────────┐
///   │                  ChatRoom                          │
///   │  ┌──────────────────────────────────────────────┐ │
///   │  │  _messageController (broadcast)              │ │
///   │  │     ↑ send()  ↑ sendError()                  │ │
///   │  └──────────────────────────────────────────────┘ │
///   │      │ stream                                     │
///   │      ├─ messages        (all data events)        │
///   │      ├─ messagesFor(u)  (filtered by recipient)  │
///   │      ├─ unreadCount     (scan accumulator)       │
///   │      └─ typingStream    (debounced indicators)   │
///   └────────────────────────────────────────────────────┘

import 'dart:async';
import 'dart:math';

// ═══════════════════════════════════════════════════════════════════════════
// Domain model
// ═══════════════════════════════════════════════════════════════════════════

enum MessageType { text, image, system }

class ChatMessage {
  final String id;
  final String sender;
  final String? recipient; // null = broadcast to all
  final String content;
  final MessageType type;
  final DateTime timestamp;

  ChatMessage({
    required this.sender,
    required this.content,
    this.recipient,
    this.type = MessageType.text,
  })  : id = _uid(),
        timestamp = DateTime.now();

  @override
  String toString() {
    final to = recipient != null ? ' → ${recipient!}' : ' → ALL';
    return '[${timestamp.millisecondsSinceEpoch % 100000}ms] '
        '${sender}$to: "$content"';
  }
}

int _uidCounter = 0;
String _uid() => 'msg_${++_uidCounter}';

// ═══════════════════════════════════════════════════════════════════════════
// ChatRoom — the stream-powered hub
// ═══════════════════════════════════════════════════════════════════════════

class ChatRoom {
  final String name;
  final _messageController = StreamController<ChatMessage>.broadcast();
  final _typingController = StreamController<String>.broadcast();

  ChatRoom(this.name);

  // ── write side ───────────────────────────────────────────────────────────

  void send(ChatMessage message) {
    if (message.content.trim().isEmpty) {
      _messageController.addError(
        ArgumentError('Message from ${message.sender} is empty'),
      );
      return;
    }
    _messageController.add(message);
  }

  void indicateTyping(String user) => _typingController.add(user);

  // ── read side ────────────────────────────────────────────────────────────

  /// All valid messages (broadcast).
  Stream<ChatMessage> get messages => _messageController.stream;

  /// Only messages addressed to [user] or to everyone.
  Stream<ChatMessage> messagesFor(String user) => messages.where(
        (m) => m.recipient == null || m.recipient == user,
      );

  /// Messages from a specific sender.
  Stream<ChatMessage> messagesFrom(String sender) =>
      messages.where((m) => m.sender == sender);

  /// Running unread count for [user] (resets on listen, not persisted).
  Stream<int> unreadCount(String user) =>
      messagesFor(user).scan<int>(0, (count, _) => count + 1);

  /// Typing indicator: debounced per user, emits the user's name.
  Stream<String> get typingIndicator =>
      _typingController.stream.debounce(Duration(milliseconds: 200));

  // ── lifecycle ────────────────────────────────────────────────────────────

  Future<void> close() async {
    await _messageController.close();
    await _typingController.close();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Simulated bots
// ═══════════════════════════════════════════════════════════════════════════

final _real = Random(42);

Stream<ChatMessage> bot(
  String name,
  ChatRoom room, {
  required List<String> messages,
  int intervalMs = 200,
}) async* {
  for (final text in messages) {
    await Future.delayed(
      Duration(milliseconds: intervalMs + _real.nextInt(intervalMs)),
    );
    room.indicateTyping(name);
    await Future.delayed(Duration(milliseconds: 100));

    final msg = ChatMessage(sender: name, content: text);
    room.send(msg);
    yield msg;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Extensions reused from Lesson 8
// ═══════════════════════════════════════════════════════════════════════════

extension DebounceExt<T> on Stream<T> {
  Stream<T> debounce(Duration duration) {
    Timer? timer;
    final c = StreamController<T>.broadcast();
    listen(
      (e) { timer?.cancel(); timer = Timer(duration, () => c.add(e)); },
      onDone: () { timer?.cancel(); c.close(); },
    );
    return c.stream;
  }
}

extension ScanExt<T> on Stream<T> {
  Stream<S> scan<S>(S seed, S Function(S acc, T e) fn) async* {
    var acc = seed;
    await for (final e in this) { acc = fn(acc, e); yield acc; }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Demo runners
// ═══════════════════════════════════════════════════════════════════════════

Future<void> runBasicChatDemo(ChatRoom room) async {
  print('\n--- Demo 1: Basic message exchange ---');

  final subs = <StreamSubscription>[];

  // Alice watches all messages
  subs.add(room.messagesFor('alice').listen(
    (m) => print('  [alice sees] $m'),
    onError: (e) => print('  [alice error] $e'),
  ));

  // Bob watches all messages
  subs.add(room.messagesFor('bob').listen(
    (m) => print('  [bob sees] $m'),
    onError: (e) => print('  [bob error] $e'),
  ));

  // Track unread for alice
  subs.add(room.unreadCount('alice').listen(
    (n) => print('  [alice unread] $n'),
    onError: (_) {}, // errors are already logged by alice's main listener
  ));

  // Typing indicator log
  subs.add(room.typingIndicator.listen(
    (user) => print('  ✎ $user is typing…'),
  ));

  // Start bots
  final aliceBot = bot('alice', room, messages: [
    'Hello everyone!',
    'How is everyone doing?',
  ]);

  final bobBot = bot('bob', room, messages: [
    'Hey Alice!',
    'All good, thanks!',
    '', // empty — triggers an error
    'Dart streams are awesome.',
  ]);

  // Run bots concurrently
  await Future.wait([
    aliceBot.drain<void>(),
    bobBot.drain<void>(),
  ]);

  // Allow final events to propagate
  await Future.delayed(Duration(milliseconds: 500));

  for (final s in subs) await s.cancel();
}

Future<void> runDirectMessageDemo(ChatRoom room) async {
  print('\n--- Demo 2: Direct messages ---');

  final completer = Completer<void>();

  // Charlie only wants messages addressed to him
  room.messagesFor('charlie').take(2).listen(
    (m) => print('  [charlie DM] $m'),
    onDone: () => completer.complete(),
  );

  room.send(ChatMessage(sender: 'alice', content: 'Hi everyone again'));
  room.send(ChatMessage(
    sender: 'bob',
    content: 'Hey Charlie, secret message!',
    recipient: 'charlie',
  ));
  room.send(ChatMessage(
    sender: 'alice',
    content: 'Charlie, another one for you',
    recipient: 'charlie',
  ));

  await completer.future;
}

Future<void> runAnalyticsDemo(ChatRoom room) async {
  print('\n--- Demo 3: Analytics — message rate per sender ---');

  final counts = <String, int>{};
  final sub = room.messages.listen((m) {
    counts[m.sender] = (counts[m.sender] ?? 0) + 1;
  });

  // Flood room with messages
  for (var i = 0; i < 5; i++) {
    room.send(ChatMessage(sender: 'alice', content: 'msg $i'));
    room.send(ChatMessage(sender: 'bob', content: 'reply $i'));
    room.send(ChatMessage(sender: 'charlie', content: 'chime $i'));
  }

  await Future.delayed(Duration(milliseconds: 50));
  await sub.cancel();

  print('  Message counts:');
  for (final entry in counts.entries) {
    print('    ${entry.key}: ${entry.value} messages');
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Entry point
// ═══════════════════════════════════════════════════════════════════════════

void main() async {
  print('=== Lesson 9: Real-World Application — Live Chat System ===');

  final room = ChatRoom('dart-streams');

  await runBasicChatDemo(room);
  await runDirectMessageDemo(room);
  await runAnalyticsDemo(room);

  await room.close();

  print('''

=== End of Lesson 9 ===

What we built:
  ✓ A broadcast StreamController as a shared message bus
  ✓ Filtered streams per user (where)
  ✓ Running unread count (scan)
  ✓ Typing indicator (debounce)
  ✓ Error channel for invalid messages (addError / onError)
  ✓ Bot generators (async*)
  ✓ Concurrent bot execution (Future.wait)
  ✓ Clean subscription management (cancel on teardown)

Stream concepts used in this lesson:
  Lesson 2 → StreamController.broadcast()
  Lesson 3 → listen() with onError
  Lesson 4 → where(), take()
  Lesson 5 → async* / yield (bots)
  Lesson 7 → addError, onError
  Lesson 8 → debounce, scan, Future.wait

=== Course Complete! ===

Lesson map:
  01 Introduction      — what streams are and why they exist
  02 Creating Streams  — factory constructors
  03 Listening         — listen, await for, StreamSubscription
  04 Transformations   — map, where, asyncMap, expand, distinct …
  05 Async Generators  — async* / yield / yield*
  06 StreamController  — push-based streams, single vs broadcast
  07 Error Handling    — handleError, onError, retry, zones
  08 Advanced Patterns — merge, zip, debounce, switchMap, scan …
  09 Real World        — live chat combining all concepts

Further reading:
  • dart.dev/libraries/async/using-streams
  • dart.dev/libraries/async/creating-streams
  • pub.dev/packages/rxdart  (rich operator library)
''');
}
