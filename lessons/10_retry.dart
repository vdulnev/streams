import 'dart:async';

// Імітація нестабільного з'єднання
Stream<String> unstableSource() async* {
  yield "З'єднання встановлено...";
  await Future.delayed(Duration(seconds: 1));
  yield "Отримано пакет даних #1";
  await Future.delayed(Duration(seconds: 1));
  throw Exception("Помилка мережі!"); // Стрім обривається
}

Stream<String> retryStream({int maxAttempts = 3}) {
  return Stream<String>.multi((controller) async {
    int attempts = 0;
    bool success = false;

    while (!success) {
        success = true;
        attempts++;
        // Перенаправляємо всі події з нестабільного джерела слухачеві
        await controller.addStream(unstableSource().handleError((e) {
          print(e);
          success = false;
        } ));
        if (attempts >= maxAttempts) {
          controller.addError("Вичерпано ліміт спроб.");
          controller.close();
          break;
        } else {
          controller.add(
              "Помилка. Спроба $attempts з $maxAttempts... Чекаємо 2 сек.");
          await Future.delayed(
              Duration(seconds: 2)); // Пауза перед наступною спробою
        }
      }

    if (!controller.isClosed) controller.close();
  });
}

void main() async {
  try {
    await for (var msg in retryStream()) {
      try {
        print(msg);
      } catch (e) {
        print('error in loop $e');
      }
    }
  } catch (e) {
    print('global error $e');
  }
}
