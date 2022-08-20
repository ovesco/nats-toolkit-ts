module.exports = {
  extends: 'airbnb-typescript-prettier',
  rules: {
    'no-restricted-syntax': ['off'],
    'prettier/prettier': [
      'error',
      {
        endOfLine: 'auto',
      },
    ],
  },
};
