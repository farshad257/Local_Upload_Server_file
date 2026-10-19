import uuid
from pathlib import Path

import aiofiles
from aiohttp import web
from aiolimiter import AsyncLimiter


# -------------------------
# Configuration
# -------------------------

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"

CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB
MAX_FILE_SIZE = 5 * 1024**3   # 5 GB per file

# Maximum 5 incoming upload requests per 60 seconds.
REQUEST_LIMITER = AsyncLimiter(max_rate=5, time_period=60)

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def remove_partial_file(file_path: Path) -> None:
    """Remove an incomplete file if an upload fails."""
    try:
        file_path.unlink(missing_ok=True)
    except OSError:
        pass


async def save_file_in_chunks(part, destination: Path) -> tuple[int, int]:
    """
    Read the incoming file in chunks of up to 1 MB
    and write them incrementally to a single local file.

    Returns:
        (total_bytes_written, number_of_chunks)
    """
    total_size = 0
    chunk_count = 0

    try:
        async with aiofiles.open(destination, "wb") as output_file:
            while True:
                # Read at most 1 MB from the incoming request.
                chunk = await part.read_chunk(size=CHUNK_SIZE)

                if not chunk:
                    break

                new_size = total_size + len(chunk)

                if new_size > MAX_FILE_SIZE:
                    raise web.HTTPRequestEntityTooLarge(
                        max_size=MAX_FILE_SIZE,
                        actual_size=new_size,
                        text="File exceeds the configured size limit.",
                    )

                # Write this chunk before reading the next one.
                await output_file.write(chunk)

                total_size = new_size
                chunk_count += 1

    except web.HTTPException:
        remove_partial_file(destination)
        raise

    except OSError as exc:
        remove_partial_file(destination)
        raise web.HTTPInternalServerError(
            text="Could not write the uploaded file to disk."
        ) from exc

    except Exception:
        remove_partial_file(destination)
        raise

    return total_size, chunk_count


async def handle_upload(request: web.Request) -> web.Response:
    """Receive files through multipart/form-data and save them locally."""

    if not request.content_type.startswith("multipart/"):
        raise web.HTTPUnsupportedMediaType(
            text="Use multipart/form-data to upload files."
        )

    # Apply the rate limit once per incoming HTTP request,
    # not once for every 1 MB file chunk.
    await REQUEST_LIMITER.acquire()

    try:
        reader = await request.multipart()
    except (ValueError, RuntimeError) as exc:
        raise web.HTTPBadRequest(
            text="Invalid multipart request."
        ) from exc

    uploaded_files = []

    async for part in reader:
        if not part.filename:
            # Ignore ordinary form fields.
            await part.release()
            continue

        # Remove any client-supplied directory components.
        original_filename = Path(
            part.filename.replace("\\", "/")
        ).name

        if not original_filename or original_filename in {".", ".."}:
            raise web.HTTPBadRequest(text="Invalid filename.")

        # Avoid overwriting another upload with the same filename.
        unique_name = f"{uuid.uuid4().hex}_{original_filename}"
        destination = UPLOAD_DIR / unique_name

        file_size, chunk_count = await save_file_in_chunks(
            part=part,
            destination=destination,
        )

        uploaded_files.append({
            "original_filename": original_filename,
            "stored_filename": unique_name,
            "size_bytes": file_size,
            "chunks_written": chunk_count,
            "chunk_size_bytes": CHUNK_SIZE,
            "storage_path": str(destination),
        })

    if not uploaded_files:
        raise web.HTTPBadRequest(text="No file was uploaded.")

    return web.json_response({
        "status": "success",
        "message": "File(s) saved locally.",
        "files": uploaded_files,
    })


# -------------------------
# Application
# -------------------------

app = web.Application(
    client_max_size=MAX_FILE_SIZE
)

app.add_routes([
    web.post("/upload", handle_upload),
])


if __name__ == "__main__":
    print("Local upload server is starting...")
    print("Endpoint: http://127.0.0.1:8080/upload")
    print(f"Upload directory: {UPLOAD_DIR}")

    web.run_app(
        app,
        host="127.0.0.1",
        port=8080,
    )