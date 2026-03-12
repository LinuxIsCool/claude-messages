import { defineConfig } from 'vitest/config';
import { readFileSync } from 'node:fs';

export default defineConfig({
  test: {
    globals: false,
  },
  plugins: [
    {
      name: 'csv-loader',
      transform(_code, id) {
        if (id.endsWith('.csv')) {
          const content = readFileSync(id, 'utf-8');
          return `export default ${JSON.stringify(content)};`;
        }
      },
    },
  ],
});
