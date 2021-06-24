#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath("../"))
from conf import *
extensions.append('sphinx_gitstamp')
extensions.append('sphinx_copybutton')
html_theme = 'sphinx_material'
html_sidebars['**']=['globaltoc.html', 'searchbox.html', 'localtoc.html', 'logo-text.html']
html_theme_options = {
    'base_url': 'http://bashtage.github.io/sphinx-material/',
    'repo_url': 'https://github.com/percona/percona-backup-mongodb',
    'repo_name': 'percona/percona-backup-mongodb',
    'color_accent': 'grey',
    'color_primary': 'orange'
}
html_logo = '../_images/percona-logo.svg'
html_favicon = '../_images/percona_favicon.ico'
pygments_style = 'emacs'
gitstamp_fmt = "%b %d, %Y"
copybutton_prompt_text = '$'
plantuml = 'java -jar ../../bin/plantuml.jar'
#html_last_updated_fmt = ''