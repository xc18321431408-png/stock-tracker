import re

with open("app.py", "r") as f:
    content = f.read()

# Extract SUB_SECTOR names - look for lines like ("category", "SYMBOL", "name")
# We want the 3rd element (name)
sub_sectors = []
for line in content.split('\n'):
    line = line.strip()
    if line.startswith('(') and line.endswith(')'):
        # Count quotes to find the name
        parts = line.split('"')
        if len(parts) >= 6:
            name = parts[5]  # 0:" 1:category 2:" 3:, 4:" 5:symbol 6:" 7:, 8:" 9:name
            # Actually let's parse more carefully
            pass

# Better approach: find the two data structures
sub_start = content.find('SUB_SECTORS = [')
stock_start = content.find('SECTOR_STOCKS = {')

sub_text = content[sub_start:stock_start]
stock_text = content[stock_start:]

# Extract sector names from SUB_SECTORS
sector_names = set()
for line in sub_text.split('\n'):
    line = line.strip()
    if line.startswith('("') or line.startswith("('"):
        # Find the last quoted string
        matches = re.findall(r'"([^"]+)"', line)
        if len(matches) >= 3:
            sector_names.add(matches[2])

# Extract keys from SECTOR_STOCKS
stock_keys = set()
for line in stock_text.split('\n'):
    line = line.strip()
    if line.startswith('"') and ':' in line and '[' not in line:
        key = line.split('"')[1]
        stock_keys.add(key)

print(f"Total sub-sectors: {len(sector_names)}")
print(f"Sectors with Top10 data: {len(stock_keys)}")
print("\nMissing sectors (no Top10 stocks):")
for s in sorted(sector_names):
    if s not in stock_keys:
        print(f"  - {s}")
