module.exports = {
  docs: [{
    type: "doc",
    id: "getting_started"
  }, {
    type: "doc",
    id: "emr_deployment"
  }, {
    type: "category",
    label: "Docusaurus",
    items: ['doc1', 'doc2', 'doc3', {
      type: "category",
      label: "Features",
      items: ["mdx"],
      collapsed: true,
    }],
    collapsed: false,
  }]
};
