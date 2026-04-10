/**
 * marimo-jupyter-scheduler — JupyterLab frontend entry point.
 *
 * Registers:
 *   - A left-sidebar panel icon (calendar icon)
 *   - A main-area Dashboard widget opened from the sidebar or the Commands palette
 */

import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin,
} from '@jupyterlab/application';
import { ICommandPalette, MainAreaWidget } from '@jupyterlab/apputils';
import { ILauncher } from '@jupyterlab/launcher';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { LabIcon } from '@jupyterlab/ui-components';
import { Widget } from '@lumino/widgets';
import React from 'react';
import ReactDOM from 'react-dom';
import { Dashboard } from './dashboard';

// ─── Plugin ID ───────────────────────────────────────────────────────────────

const PLUGIN_ID = 'marimo-jupyter-scheduler:plugin';
const COMMAND_OPEN = 'marimo-scheduler:open-dashboard';

// ─── Icon ─────────────────────────────────────────────────────────────────────

const ICON_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
  <defs>
    <clipPath id="marimo-cal-clip">
      <rect x="5" y="12" width="90" height="82" rx="10"/>
    </clipPath>
  </defs>
  <!-- Drop shadow -->
  <rect x="7" y="15" width="90" height="82" rx="10" fill="#00000018"/>
  <!-- Calendar body -->
  <rect x="5" y="12" width="90" height="82" rx="10" fill="#fafafa"/>
  <!-- Orange header (Marimo brand) -->
  <rect x="5" y="12" width="90" height="34" fill="#FF6D3B" clip-path="url(#marimo-cal-clip)"/>
  <!-- Binding pegs -->
  <rect x="27" y="4" width="8" height="20" rx="4" fill="#555"/>
  <rect x="65" y="4" width="8" height="20" rx="4" fill="#555"/>
  <!-- Bold "m" in white -->
  <text x="50" y="36"
        text-anchor="middle"
        font-family="Helvetica Neue, Helvetica, Arial, sans-serif"
        font-weight="900"
        font-size="25"
        fill="white">m</text>
  <!-- Calendar day grid (5 × 3) -->
  <circle cx="21" cy="59" r="4" fill="#ddd"/>
  <circle cx="36" cy="59" r="4" fill="#ddd"/>
  <circle cx="50" cy="59" r="4" fill="#ddd"/>
  <circle cx="64" cy="59" r="4" fill="#ddd"/>
  <circle cx="79" cy="59" r="4" fill="#ddd"/>
  <circle cx="21" cy="74" r="4" fill="#ddd"/>
  <circle cx="36" cy="74" r="4" fill="#ddd"/>
  <circle cx="50" cy="74" r="4.5" fill="#FF6D3B"/>
  <circle cx="64" cy="74" r="4" fill="#ddd"/>
  <circle cx="79" cy="74" r="4" fill="#ddd"/>
  <circle cx="21" cy="89" r="4" fill="#ddd"/>
  <circle cx="36" cy="89" r="4" fill="#ddd"/>
  <circle cx="50" cy="89" r="4" fill="#ddd"/>
</svg>
`.trim();

export const marimoSchedulerIcon = new LabIcon({
  name: 'marimo-scheduler:calendar',
  svgstr: ICON_SVG,
});

// ─── Lumino widget wrapping the React dashboard ───────────────────────────────

class DashboardWidget extends Widget {
  constructor() {
    super();
    this.id = 'marimo-scheduler-dashboard';
    this.title.label = 'Marimo Scheduler';
    this.title.icon = marimoSchedulerIcon;
    this.title.closable = true;
    this.addClass('jp-MarimoSchedulerDashboard');
  }

  onAfterAttach(): void {
    this._render();
  }

  onAfterShow(): void {
    this._render();
  }

  private _render(): void {
    ReactDOM.render(React.createElement(Dashboard), this.node);
  }

  dispose(): void {
    ReactDOM.unmountComponentAtNode(this.node);
    super.dispose();
  }
}

// ─── Plugin ──────────────────────────────────────────────────────────────────

const plugin: JupyterFrontEndPlugin<void> = {
  id: PLUGIN_ID,
  description: 'Schedule Marimo notebooks via JupyterLab',
  autoStart: true,
  optional: [ICommandPalette, ISettingRegistry, ILauncher],
  activate: (
    app: JupyterFrontEnd,
    palette: ICommandPalette | null,
    _settings: ISettingRegistry | null,
    launcher: ILauncher | null
  ) => {
    let widget: MainAreaWidget<DashboardWidget> | null = null;

    // Helper: create or reveal the dashboard
    const openDashboard = (): void => {
      if (!widget || widget.isDisposed) {
        const content = new DashboardWidget();
        widget = new MainAreaWidget({ content });
        widget.id = 'marimo-scheduler-main';
        widget.title.label = 'Marimo Scheduler';
        widget.title.icon = marimoSchedulerIcon;
        widget.title.closable = true;
      }

      if (!widget.isAttached) {
        app.shell.add(widget, 'main');
      }

      app.shell.activateById(widget.id);
    };

    // Register command
    app.commands.addCommand(COMMAND_OPEN, {
      label: 'Marimo Scheduler',
      caption: 'View and manage scheduled Marimo notebook jobs',
      icon: marimoSchedulerIcon,
      execute: openDashboard,
    });

    // Add to command palette
    if (palette) {
      palette.addItem({
        command: COMMAND_OPEN,
        category: 'Marimo Scheduler',
      });
    }

    // Add launcher card (shows up on the JupyterLab home/launcher tab)
    if (launcher) {
      launcher.add({
        command: COMMAND_OPEN,
        category: 'Other',
        rank: 3,
      });
    }

    console.log('marimo-jupyter-scheduler: activated');
  },
};

export default plugin;
