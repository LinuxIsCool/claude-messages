declare module 'parse-full-name' {
  export function parseFullName(name: string): {
    title: string;
    first: string;
    middle: string;
    last: string;
    nick: string;
    suffix: string;
    error: string[];
  };
}
