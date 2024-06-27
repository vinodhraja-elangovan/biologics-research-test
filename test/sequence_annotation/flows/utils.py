from typing import Optional


def sanitize_sequence(sequence: Optional[str]) -> Optional[str]:
    return sequence.replace('-', '') if sequence and isinstance(sequence, str) else None
