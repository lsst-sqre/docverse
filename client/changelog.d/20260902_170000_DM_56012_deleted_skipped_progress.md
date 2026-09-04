### New features

- `BuildProcessingProgress` gains a `deleted_skipped` field. It is `true` on a `build_processing` job that completed without publishing anything because the build was deleted — whether the delete landed before the worker picked the job up or while the worker was still uploading — so the build is `cancelled` rather than published. The key already round-tripped through `extra="allow"`; declaring it puts the flag in the generated OpenAPI schema so callers can tell this outcome apart from a real publish.
