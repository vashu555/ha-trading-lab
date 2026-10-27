"""Read-only Python syntax validation; needs only Python's standard library."""
import ast
from pathlib import Path

root=Path(__file__).resolve().parents[1]
count=0
for path in sorted(root.rglob('*.py')):
    if any(p in {'.git','tmp','__pycache__','.venv','venv'} for p in path.relative_to(root).parts):
        continue
    ast.parse(path.read_text(encoding='utf-8-sig'),filename=str(path))
    count+=1
print(f'PASS: {count} Python files parsed successfully')
