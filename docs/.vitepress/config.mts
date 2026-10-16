import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(
  defineConfig({
    title: 'AetherDB',
    description: 'A Raft-replicated key-value database built from scratch in C++17, with a Spring Boot gateway.',
    cleanUrls: true,
    lastUpdated: true,
    // README.md is the GitHub landing page for this folder, not a site page.
    srcExclude: ['README.md'],
    // Local URLs like http://localhost:8080 are instructions, not links to
    // check; every other dead link still fails the build.
    ignoreDeadLinks: 'localhostLinks',
    head: [['meta', { name: 'theme-color', content: '#2f5d8a' }]],
    themeConfig: {
      nav: [
        { text: 'Guide', link: '/getting-started' },
        { text: 'Internals', link: '/architecture' },
        { text: 'Findings', link: '/findings' },
        { text: 'GitHub', link: 'https://github.com/Aravind9548/Aether_DB' }
      ],
      sidebar: [
        {
          text: 'Introduction',
          items: [
            { text: 'Overview', link: '/' },
            { text: 'Getting started', link: '/getting-started' }
          ]
        },
        {
          text: 'How it works',
          items: [
            { text: 'Architecture', link: '/architecture' },
            { text: 'Consensus (Raft)', link: '/raft' },
            { text: 'Request flows', link: '/flows' },
            { text: 'Design decisions', link: '/design-decisions' }
          ]
        },
        {
          text: 'Reference',
          items: [
            { text: 'API reference', link: '/api' },
            { text: 'Configuration & operations', link: '/operations' }
          ]
        },
        {
          text: 'Verification',
          items: [
            { text: 'Testing', link: '/testing' },
            { text: 'Findings: three Raft bugs', link: '/findings' }
          ]
        },
        {
          text: 'More',
          items: [
            { text: 'Limitations & roadmap', link: '/limitations' },
            { text: 'Glossary', link: '/glossary' }
          ]
        }
      ],
      socialLinks: [{ icon: 'github', link: 'https://github.com/Aravind9548/Aether_DB' }],
      search: { provider: 'local' },
      outline: { level: [2, 3] },
      footer: {
        message: 'AetherDB: a distributed systems project.',
        copyright: 'Aravind Reddy'
      }
    }
  })
)
