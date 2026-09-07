/**
 * Interleave results for infinite scrolling
 * Takes an array of arrays and interleaves them.
 * @param {Array} articles - Array of arrays
 * @param {number} chunkSize - Number of elements to chunk by
 * @returns {Array} - Interleaved array
 */
function interleaveResults(articles, chunkSize) {
  const chunks = [];
  for (let i = 0; i < articles.length; i += chunkSize) {
    chunks.push(articles.slice(i, i + chunkSize));
  }

  const interleaved = [];
  const maxLength = Math.max(...chunks.map(c => c.length));

  for (let i = 0; i < maxLength; i++) {
    chunks.forEach(chunk => {
      if (chunk[i]) interleaved.push(chunk[i]);
    });
  }

  return interleaved;
}

module.exports = {
  interleaveResults
};
