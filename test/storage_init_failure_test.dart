// Regression test for MOBILE-NEWS-PY.
//
// `BaseDownloader.instance` started initialization with a bare
// `unawaited(instance.initialize())`. `unawaited` only silences the lint - it
// attaches no error handler - so when the persistent storage failed to open
// (on Windows, a lock violation reading the localstore database at startup)
// the error escaped to the zone handler and killed the host app. A downloader
// that cannot open its database must degrade, not crash.
//
// Second failure in the same path: `_readyCompleter` was only completed on the
// success branch, so every `await ready` - including `FileDownloader()
// .trackTasks()` - hung forever after a failed init.

import 'dart:async';

import 'package:background_downloader/background_downloader.dart';
import 'package:background_downloader/src/base_downloader.dart';
import 'package:flutter_test/flutter_test.dart';

/// Storage whose [initialize] fails the way the localstore isolate fails: with
/// a stringified error, which is what crossing the isolate boundary produces.
class _FailingStorage implements PersistentStorage {
  static const failure =
      "PathAccessException: readInto failed, path = 'metaData' "
      '(OS Error: lock violation, errno = 33)';

  @override
  Future<void> initialize() => Future.error(failure);

  @override
  (String, int) get currentDatabaseVersion => ('Failing', 1);

  @override
  Future<(String, int)> get storedDatabaseVersion => Future.error(failure);

  @override
  Future<void> storeTaskRecord(TaskRecord record) async {}

  @override
  Future<TaskRecord?> retrieveTaskRecord(String taskId) async => null;

  @override
  Future<List<TaskRecord>> retrieveAllTaskRecords() async => [];

  @override
  Future<void> removeTaskRecord(String? taskId) async {}

  @override
  Future<void> storePausedTask(Task task) async {}

  @override
  Future<Task?> retrievePausedTask(String taskId) async => null;

  @override
  Future<List<Task>> retrieveAllPausedTasks() async => [];

  @override
  Future<void> removePausedTask(String? taskId) async {}

  @override
  Future<void> storeResumeData(ResumeData resumeData) async {}

  @override
  Future<ResumeData?> retrieveResumeData(String taskId) async => null;

  @override
  Future<List<ResumeData>> retrieveAllResumeData() async => [];

  @override
  Future<void> removeResumeData(String? taskId) async {}
}

void main() {
  // Database and the platform downloaders are process-wide singletons, so a
  // BaseDownloader can only be built once per test file.
  test('a failing storage init neither crashes nor deadlocks', () async {
    TestWidgetsFlutterBinding.ensureInitialized();
    final storage = _FailingStorage();

    final zoneErrors = <Object>[];
    final downloader = await runZonedGuarded(
      () async => BaseDownloader.instance(storage, Database(storage)),
      (e, s) => zoneErrors.add(e),
    )!;

    // `ready` resolves false instead of hanging: everything behind
    // `await ready` fails fast rather than waiting forever.
    expect(
      await downloader.ready.timeout(const Duration(seconds: 5)),
      isFalse,
    );

    // Give any stray error a turn of the loop to surface before asserting.
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(zoneErrors, isEmpty,
        reason: 'storage init failure must not reach the zone error handler');
  });
}
