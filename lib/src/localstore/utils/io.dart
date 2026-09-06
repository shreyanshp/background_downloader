import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:path/path.dart' as p;

import '../localstore.dart';
import 'utils_impl.dart';
import 'package:logging/logging.dart';

final _log = Logger('Localstore');

final class Utils implements UtilsImpl {
  Utils._();

  static final Utils _utils = Utils._();
  static final lastPathComponentRegEx = RegExp(r'[^/\\]+[/\\]?$');

  static Utils get instance => _utils;

  /// Backoff before read attempt N (0-based); its length sets how many times
  /// a read is retried before the lock error is rethrown. See [_readBytes].
  static const _readBackoff = [
    Duration(milliseconds: 15),
    Duration(milliseconds: 30),
    Duration(milliseconds: 60),
  ];
  final _storageCache = <String, StreamController<Map<String, dynamic>>>{};
  final _fileCache = <String, File>{};
  final _locks = <String, Future<void>>{};

  /// Clears the cache
  @override
  void clearCache() {
    _storageCache.clear();
    _fileCache.clear();
  }

  Future<T> _synchronized<T>(String path, Future<T> Function() action) async {
    final previous = _locks[path] ?? Future.value();
    final controller = Completer<T>();

    final newFuture = previous.then((_) async {
      try {
        final result = await action();
        controller.complete(result);
      } catch (e, st) {
        controller.completeError(e, st);
      }
    }).catchError((_) {});

    _locks[path] = newFuture;

    // Cleanup
    newFuture.whenComplete(() {
      if (_locks[path] == newFuture) {
        _locks.remove(path);
      }
    });

    return controller.future;
  }

  String _resolvePath(String dbPath, String path) {
    var relativePath = path;
    if (relativePath.startsWith('/') || relativePath.startsWith('\\')) {
      relativePath = relativePath.substring(1);
    }
    return p.join(dbPath, relativePath);
  }

  @override
  Future<Map<String, dynamic>?> get(
    String path, [
    bool? isCollection = false,
    List<List>? conditions,
  ]) async {
    // Fetch the documents for this collection
    if (isCollection != null && isCollection == true) {
      final dbDir = await Localstore.instance.databaseDirectory;
      final fullPath = _resolvePath(dbDir.path, path);
      final dir = Directory(fullPath);
      if (!await dir.exists()) {
        return {};
      }
      final entries = await dir.list(recursive: false).toList();
      return await _getAll(entries.whereType<File>().toList());
    } else {
      return _synchronized(path, () async {
        try {
          // Reads the document referenced by this [DocumentRef].
          final file = await _getFile(path);
          final randomAccessFile = await file!.open(mode: FileMode.append);
          try {
            final data = await _readFile(randomAccessFile);
            if (data is Map<String, dynamic>) {
              final key = path.replaceAll(lastPathComponentRegEx, '');
              // ignore: close_sinks
              final storage = _storageCache.putIfAbsent(
                key,
                () => _newStream(key),
              );
              storage.add(data);
              return data;
            }
          } finally {
            await randomAccessFile.close();
          }
        } on PathNotFoundException {
          // return null if not found
        } on PathAccessException catch (e) {
          // The OS refused access - on Windows a write holding the range
          // (ERROR_LOCK_VIOLATION, errno 33), which [_readBytes] has already
          // retried. Report it as "no document yet" rather than let a
          // transient lock escape: every caller already handles null, and an
          // escaping error here reaches the zone handler and is fatal.
          _log.warning('Could not read $path: $e');
        }
        return null;
      });
    }
  }

  @override
  Future<dynamic>? set(Map<String, dynamic> data, String path) {
    return _writeFile(data, path);
  }

  @override
  Future delete(String path) async {
    if (path.endsWith(Platform.pathSeparator)) {
      await _deleteDirectory(path);
    } else {
      await _synchronized(path, () => _deleteFile(path));
    }
  }

  @override
  Stream<Map<String, dynamic>> stream(String path, [List<List>? conditions]) {
    // ignore: close_sinks
    var storage = _storageCache[path];
    if (storage == null) {
      storage = _storageCache.putIfAbsent(path, () => _newStream(path));
    } else {
      _initStream(storage, path);
    }
    return storage.stream;
  }

  Future<Map<String, dynamic>?> _getAll(List<FileSystemEntity> entries) async {
    _log.finest('Getting all entries from ${entries.length} files');
    final items = <String, dynamic>{};
    final dbDir = await Localstore.instance.databaseDirectory;
    for (final e in entries) {
      final relativePath = p.relative(e.path, from: dbDir.path);
      final path = Platform.isWindows
          ? relativePath.replaceAll(p.separator, '/')
          : relativePath;

      await _synchronized(path, () async {
        final file = await _getFile(path);
        try {
          final randomAccessFile = await file!.open(mode: FileMode.append);
          try {
            final data = await _readFile(randomAccessFile);
            if (data is Map<String, dynamic>) {
              items[path] = data;
            }
          } finally {
            await randomAccessFile.close();
          }
        } on PathNotFoundException {
          // ignore if not found
        } catch (e) {
          _log.warning('Error reading file $path: $e');
        }
      });
    }

    if (items.isEmpty) return null;
    return items;
  }

  /// Streams all file in the path
  StreamController<Map<String, dynamic>> _newStream(String path) {
    final storage = StreamController<Map<String, dynamic>>.broadcast();
    _initStream(storage, path);

    return storage;
  }

  Future _initStream(
    StreamController<Map<String, dynamic>> storage,
    String path,
  ) async {
    final dbDir = await Localstore.instance.databaseDirectory;
    final fullPath = _resolvePath(dbDir.path, path);
    final dir = Directory(fullPath);
    try {
      if (!await dir.exists()) return;
      final entries = await dir.list(recursive: false).toList();
      for (var e in entries) {
        if (e is! File) continue;
        final relativePath = p.relative(e.path, from: dbDir.path);
        final filePath = Platform.isWindows
            ? relativePath.replaceAll(p.separator, '/')
            : relativePath;

        // We use synchronized reading
        await _synchronized(filePath, () async {
          final file = await _getFile(filePath);
          final randomAccessFile = await file!.open(mode: FileMode.append);
          try {
            final data = await _readFile(randomAccessFile);
            if (data is Map<String, dynamic>) {
              storage.add(data);
            }
          } finally {
            await randomAccessFile.close();
          }
        });
      }
    } catch (e) {
      // ignore
    }
  }

  Future<dynamic> _readFile(RandomAccessFile file) async {
    final buffer = await _readBytes(file);
    try {
      final contentText = utf8.decode(buffer);
      final data = json.decode(contentText) as Map<String, dynamic>;
      return data;
    } catch (e) {
      return e;
    }
  }

  /// Reads every byte of [file], retrying while the OS reports the range is
  /// locked.
  ///
  /// On Windows a read that overlaps [_writeFile]'s exclusive lock fails with
  /// ERROR_LOCK_VIOLATION (errno 33). That lock is held for microseconds, so
  /// the loser of the race only has to come back a moment later. A file that
  /// is genuinely absent is not a race and is rethrown immediately, as is a
  /// lock that outlives every attempt - the caller decides what that means.
  Future<Uint8List> _readBytes(RandomAccessFile file) async {
    for (var attempt = 0;; attempt++) {
      try {
        final length = await file.length();
        await file.setPosition(0);
        final buffer = Uint8List(length);
        await file.readInto(buffer);
        return buffer;
      } on PathNotFoundException {
        rethrow;
      } on FileSystemException catch (e) {
        if (attempt >= _readBackoff.length) rethrow;
        _log.finest('Retrying locked read (attempt ${attempt + 1}): $e');
        await Future<void>.delayed(_readBackoff[attempt]);
      }
    }
  }

  Future<File?> _getFile(String path) async {
    if (_fileCache.containsKey(path)) return _fileCache[path];

    final dbDir = await Localstore.instance.databaseDirectory;
    final file = File(_resolvePath(dbDir.path, path));

    if (!await file.exists()) await file.create(recursive: true);
    _fileCache.putIfAbsent(path, () => file);

    return file;
  }

  Future _writeFile(Map<String, dynamic> data, String path) {
    return _synchronized(path, () async {
      final serialized = json.encode(data);
      final buffer = utf8.encode(serialized);
      final file = await _getFile(path);
      try {
        final randomAccessFile = await file!.open(mode: FileMode.append);
        var locked = false;
        try {
          await randomAccessFile.lock();
          locked = true;
          await randomAccessFile.setPosition(0);
          await randomAccessFile.writeFrom(buffer);
          await randomAccessFile.truncate(buffer.length);
        } finally {
          // Release only a lock we actually took, and release it even when the
          // write threw - the old code skipped the unlock on any failure.
          if (locked) {
            try {
              await randomAccessFile.unlock();
            } catch (e) {
              _log.finest('Could not unlock $path: $e');
            }
          }
          await randomAccessFile.close();
        }
      } on PathNotFoundException {
        // ignore if path not found
      } on FileSystemException catch (e) {
        // Another handle holds the range, or the file went away mid-write.
        // Skip this write instead of throwing: the record is rewritten on the
        // next status update, and an escaping error here is fatal.
        _log.warning('Could not write $path: $e');
      }
      final key = path.replaceAll(lastPathComponentRegEx, '');
      // ignore: close_sinks
      final storage = _storageCache.putIfAbsent(key, () => _newStream(key));
      storage.add(data);
    });
  }

  Future _deleteFile(String path) async {
    final dbDir = await Localstore.instance.databaseDirectory;
    final file = File(_resolvePath(dbDir.path, path));
    if (await file.exists()) {
      try {
        await file.delete();
      } catch (e) {
        _log.finest(e);
      }
    }
    _fileCache.remove(path);
  }

  Future _deleteDirectory(String path) async {
    final dbDir = await Localstore.instance.databaseDirectory;
    final dir = Directory(_resolvePath(dbDir.path, path));
    if (await dir.exists()) {
      try {
        await dir.delete(recursive: true);
      } catch (e) {
        _log.finest(e);
      }
    }
    _fileCache.removeWhere((key, value) => key.startsWith(path));
  }
}
