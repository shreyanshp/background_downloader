// Regression tests for MOBILE-NEWS-PY: a startup read of
// `backgroundDownloaderDatabase/metaData` that the OS refuses (Windows
// ERROR_LOCK_VIOLATION, errno 33, from a concurrent write holding the range)
// used to escape `Utils.get` and, because `BaseDownloader.instance` starts
// initialization with a bare `unawaited(...)`, reach the zone error handler as
// a fatal crash.
//
// Windows mandatory locking cannot be reproduced on a POSIX host - `lock()`
// there is advisory and does not block a read through a second handle. These
// tests therefore drive the same code path with the other error the OS raises
// as a `PathAccessException`: a file the process may not open. The contract
// under test is identical - a refused open/read must surface as "no document"
// rather than as an exception.

import 'dart:convert';
import 'dart:io';

import 'package:background_downloader/src/localstore/localstore.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path_provider_platform_interface/path_provider_platform_interface.dart';

class _FakePathProvider extends PathProviderPlatform {
  _FakePathProvider(this.root);

  final String root;

  @override
  Future<String?> getApplicationSupportPath() async => root;
}

void main() {
  late Directory root;

  setUpAll(() async {
    TestWidgetsFlutterBinding.ensureInitialized();
    root = await Directory.systemTemp.createTemp('localstore_test');
    // Must be installed before anything touches Localstore.instance: the
    // database directory is resolved once, in the singleton's initializer.
    PathProviderPlatform.instance = _FakePathProvider(root.path);
  });

  tearDownAll(() async {
    if (await root.exists()) {
      await root.delete(recursive: true);
    }
  });

  setUp(() => Localstore.instance.clearCache());

  /// Makes [file] unopenable and returns false if the OS ignored that (running
  /// as root), so the caller can skip rather than assert on a false premise.
  Future<bool> makeUnreadable(File file) async {
    await Process.run('chmod', ['000', file.path]);
    try {
      await (await file.open(mode: FileMode.append)).close();
      return false;
    } on FileSystemException {
      return true;
    }
  }

  File fileFor(String collection, String doc) =>
      File('${root.path}${Platform.pathSeparator}$collection'
          '${Platform.pathSeparator}$doc');

  /// Writes a document straight to disk, bypassing [DocumentRef.set].
  ///
  /// [DocumentRef.get] short-circuits to an in-memory copy of anything this
  /// process wrote, so a doc written through the API is never read back off
  /// disk. Seeding the file directly is also the real scenario: the metaData
  /// file that crashed at startup was left there by a previous run.
  Future<File> seed(String collection, String doc, Map<String, Object?> data) async {
    final file = fileFor(collection, doc);
    await file.parent.create(recursive: true);
    await file.writeAsString(jsonEncode(data));
    return file;
  }

  test('get returns null when the OS refuses the read', () async {
    final file = await seed('locked', 'metaData', {'version': 1});
    if (!await makeUnreadable(file)) {
      markTestSkipped('filesystem permissions not enforced for this user');
      return;
    }

    // Before the fix this threw PathAccessException out of Utils.get, which
    // reached the zone error handler as a fatal.
    expect(await Localstore.instance.collection('locked').doc('metaData').get(),
        isNull);

    await Process.run('chmod', ['644', file.path]);
  });

  test('set does not throw when the OS refuses the write', () async {
    final doc = Localstore.instance.collection('locked_write').doc('metaData');
    final file = await seed('locked_write', 'metaData', {'version': 1});
    if (!await makeUnreadable(file)) {
      markTestSkipped('filesystem permissions not enforced for this user');
      return;
    }

    // The write is dropped, not raised: the record is rewritten on the next
    // status update, whereas an escaping error here is fatal.
    await expectLater(doc.set({'version': 2}), completes);

    await Process.run('chmod', ['644', file.path]);
  });

  test('a document that was never written still reads as null', () async {
    final doc = Localstore.instance.collection('empty').doc('absent');
    expect(await doc.get(), isNull);
  });

  test('a readable document on disk is still read back', () async {
    await seed('roundtrip', 'metaData', {'version': 1});
    final doc = Localstore.instance.collection('roundtrip').doc('metaData');
    expect((await doc.get())?['version'], 1);
  });

  test('interleaved reads and writes of one document never throw', () async {
    await seed('race', 'metaData', {'version': 0});
    final collection = Localstore.instance.collection('race');
    final doc = collection.doc('metaData');

    // Reads go through the collection so they hit the file rather than
    // DocumentRef's in-memory copy. This exercises the _synchronized queue
    // together with the new read-retry and write-guard paths - the shape that
    // produced the lock violation on Windows. Here it is a no-throw guarantee.
    await expectLater(
      Future.wait([
        for (var i = 1; i <= 20; i++) ...[
          Future(() => doc.set({'version': i})),
          Future(() => collection.get()),
        ],
      ]),
      completes,
    );

    // The document survived the storm and still parses as a record.
    final onDisk = jsonDecode(await fileFor('race', 'metaData').readAsString());
    expect((onDisk as Map)['version'], isA<int>());
  });
}
