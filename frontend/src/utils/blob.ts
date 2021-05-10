
export function blobToBase64String(blob: string, cappedLength: number = 0): string {
  return (cappedLength <= 0) ? blob : blob.substring(0, cappedLength);
}

export function blobToHexString(blob: string, cappedLength: number = 0): string {
  const lookup = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"];
  const enc = new TextEncoder();
  const bytes = enc.encode(atob(blob));
  let str = "";
  (cappedLength <= 0 ? bytes : bytes.subarray(0, cappedLength)).forEach(byte => {
      const hex1 = byte & 0xf;
      const hex2 = (byte >> 4) & 0xf;
      str += lookup[hex1];
      str += lookup[hex2];
  });
  return str;
}

