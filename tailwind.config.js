/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/teelo/web/templates/**/*.html",
    "./src/teelo/web/static/js/**/*.js"
  ],
  theme: {
    extend: {
      colors: {
        teelo: {
          lime: '#CCFF00',
          blue: '#377DB8',
          light: '#FAFAFA', 
          dark: '#1A1A1A',
          surface: '#ffffff',
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      boxShadow: {
        'soft': '0 4px 20px -2px rgba(0, 0, 0, 0.05)',
        'glow': '0 0 15px rgba(204, 255, 0, 0.4)',
      },
      typography: (theme) => ({
        DEFAULT: {
          css: {
            color: theme('colors.gray.600'),
            maxWidth: 'none',
            '--tw-prose-headings': theme('colors.teelo.dark'),
            '--tw-prose-links': theme('colors.teelo.dark'),
            h1: {
              fontWeight: '800',
            },
            h2: {
              fontWeight: '700',
              marginTop: '1.5em',
            },
            h3: {
              fontWeight: '600',
            },
            strong: {
              color: theme('colors.teelo.dark'),
              fontWeight: '700',
            },
            a: {
              fontWeight: '600',
              textDecoration: 'underline',
              textDecorationColor: theme('colors.teelo.lime'),
              textDecorationThickness: '2px',
              '&:hover': {
                backgroundColor: theme('colors.teelo.lime'),
                color: theme('colors.teelo.dark'),
                textDecoration: 'none',
              },
            },
            code: {
              color: theme('colors.teelo.dark'),
              backgroundColor: theme('colors.teelo.lime'),
              padding: '0.2em 0.4em',
              borderRadius: '0.25rem',
              fontWeight: '500',
            },
            'code::before': { content: '""' },
            'code::after': { content: '""' },
            pre: {
              backgroundColor: theme('colors.gray.900'),
              color: theme('colors.gray.100'),
            },
            table: {
              width: '100%',
              marginTop: '1em',
              marginBottom: '1em',
            },
            th: {
              color: theme('colors.teelo.dark'),
              fontWeight: '600',
              textAlign: 'left',
            },
          },
        },
      }),
    }
  },
  plugins: [
    require('@tailwindcss/typography'),
  ],
}
