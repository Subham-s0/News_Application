/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      keyframes: {
        blink: {
          '0%, 20%': { opacity: '0' },
          '50%': { opacity: '1' },
          '100%': { opacity: '0' },
        },
      },
      animation: {
        'blink-1': 'blink 1s infinite',
        'blink-2': 'blink 1s infinite 0.2s',
        'blink-3': 'blink 1s infinite 0.4s',
      },
    },
  },
  plugins: [],
};
