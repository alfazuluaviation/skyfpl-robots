from PIL import Image
import io

# 1. Completely transparent 256x256 RGBA image (all 0)
empty_img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))

buf_png = io.BytesIO()
empty_img.save(buf_png, format="PNG", optimize=True)
png_len = len(buf_png.getvalue())
print(f"Empty 256x256 PNG size: {png_len} bytes")

# 2. Image with just a small corner (e.g. 5% chart)
wedge_img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
# Fill a small triangle in the corner
for y in range(40):
    for x in range(40 - y):
        wedge_img.putpixel((x, y), (240, 240, 240, 255))

buf_wedge_png = io.BytesIO()
wedge_img.save(buf_wedge_png, format="PNG", optimize=True)
print(f"Small corner wedge PNG size: {len(buf_wedge_png.getvalue())} bytes")
