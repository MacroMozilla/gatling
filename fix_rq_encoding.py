from pathlib import Path

def fix_requirements_encoding(file="requirements.txt"):
    path = Path(file)
    if not path.exists():
        print(f"❌ {file} not found.")
        return

    data = path.read_bytes()

    # 检测并去掉 UTF-8 BOM
    if data.startswith(b'\xef\xbb\xbf'):
        print("⚙️  Removing UTF-8 BOM...")
        data = data[3:]

    # 重新以 UTF-8 无 BOM 保存
    text = data.decode('utf-8', errors='ignore')
    path.write_text(text, encoding='utf-8')
    print(f"✅ {file} saved as UTF-8 (no BOM)")

if __name__ == "__main__":
    fix_requirements_encoding()
