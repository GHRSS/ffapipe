# type: ignore

### Standard imports. ###

import os
import curses
import pickle

### Local imports. ###

from src_scripts.utilities import (reader,
                                   unpickler)

if __name__ == '__main__':

    import argparse
    import textwrap

    #####################################################################################

    parser = argparse.ArgumentParser(
        formatter_class = argparse.RawDescriptionHelpFormatter,
        usage = argparse.SUPPRESS,
        description = textwrap.dedent('''
        ######################################
        GHRSS Survey FFA Pipeline: The Monitor
        ######################################

        Just like the cosmic being in the DC universe it is
        named after, the Monitor updates the user about both
        per-file and per-date progress of the pipeline. The
        only input is the particular date for which the user
        wishes to get an update or summary.

        To be run from the "GHRSS_FFA_Pipeline" directory only.

        usage: python the_monitor.py [date]

        '''))

    parser.add_argument('date',
                        type = str,
                        help = textwrap.dedent(
                        """ The date for which updates are required. """))

    try:

        args = parser.parse_args()

    except:

        parser.print_help()
        parser.exit(1)

    date = args.date

    #####################################################################################

    def build_record(config, summary):

        """ Build the summary of all processing done till now by the pipeline. """

        record = []
        
        # Record the configuration.

        descp_cfg = ('Current Pipeline Configuration\n'
                     '------------------------------\n' 
                     'Node(s): {nodes}\n'
                     'Machine configuration: {mach_config}\n'
                     'Dates to be analysed: {dates}\n'
                     'Number of cores per node: {cores}\n\n'
      
                     'DM range: {DM_lowest} to {DM_highest} pc cm^-3\n'
                     'Number of DM trials: {numDMs}\n'
                     'Period ranges searched by the FFA:\n'
                     '    (0.2 to 0.5 s), (0.5 to 2s) and (2 to 100s)\n\n'
                     '').format(nodes=config._config['pipeline_variables']['nodes'],
                                mach_config=config.mach_config,
                                dates=config._config['pipeline_variables']['dates'][config.backend],
                                cores=config.cores,
                                DM_lowest=config.ddplan['DM_lowest'],
                                DM_highest=config.ddplan['DM_highest'],
                                numDMs=config.ddplan['numDMs'])
        
        record.append(descp_cfg)
        
        # Then the summary.
        
        header = ['Filename',
                  'DM trials',
                  'Candidates',
                  'Folding Status',
                  'Archiving Status']
        
        fmt_hdr = ('{:<50} {:<11} {:<11} {:<15} {}\n\n').format(*header)
        
        # Record table header.
        
        record.append(fmt_hdr)
        
        for meta in summary:
        
            # Check if folding has been carried out properly.
            
            if meta['num_fold_prfs']:
            
                if (meta['num_candidates'] == meta['num_fold_prfs']):
                    fold_flag = 'Done.'
                else:
                    fold_flag = 'Incomplete.'
            
            else:
                fold_flag = 'Not Done.'
                
            # Check if archiving has been carried out properly.
                
            if meta['num_arv_prfs']:
            
                if (meta['num_fold_prfs'] == meta['num_arv_prfs']):
                    archive_flag = 'Done.'
                else:
                    archive_flag = 'Incomplete.'
            
            else:
                archive_flag = 'Not Done.'   
                
            # Construct each row of the table.                         
        
            row = ('{fname:<53}'
                   '{proc_dm_trials:<13}'
                   '{num_candidates:<13}'
                   '{fd_flag:<16}'
                   '{arv_flag}\n').format(fname=meta['fname'],
                                          proc_dm_trials=str(meta['proc_dm_trials']),
                                          num_candidates=str(meta['num_candidates']),
                                          fd_flag=fold_flag,
                                          arv_flag=archive_flag)

            # Record each row.
            
            record.append(row)

        return record

    
    def cli_updater(stdscr, date):

        """ Create a CLI where all summaries and updates are displayed.
        The CLI was built using the "curses" module in Python.
        """

        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
        
        stdscr.keypad(1)
        curses.curs_set(0)
        
        # Initialise the color combinations we're going to use.
        
        curses.init_pair(1, curses.COLOR_RED, -1)
        curses.init_pair(2, curses.COLOR_GREEN, -1)
        
        # Begin the program.
        
        stdscr.addstr('GHRSS FFA Pipeline', curses.A_REVERSE)
        stdscr.chgat(-1, curses.A_REVERSE)
        
        stdscr.addstr(curses.LINES-1, 0, ('Press "U" to request a new update, '
                                          'Press "S" to request a summary, '
                                          '"Q" to quit.'))
        
        # Change the U and the S to green and the Q to red.
        
        stdscr.chgat(curses.LINES-1, 7, 1, curses.A_BOLD | curses.color_pair(2))
        stdscr.chgat(curses.LINES-1, 42, 1, curses.A_BOLD | curses.color_pair(2))
        stdscr.chgat(curses.LINES-1, 68, 1, curses.A_BOLD | curses.color_pair(1))
        
        # Set up the window to hold the updates.
        
        update_window = curses.newwin(curses.LINES-2, curses.COLS, 1, 0)
        
        # Create a sub-window so as to cleanly display the update without worrying
        # about over-writing the update window's borders.
        
        update_text_window = update_window.subwin(curses.LINES-6, curses.COLS-4, 3, 2)
        
        update_text_window.addstr('Press "U" to get an update. Press "S" to get a summary.')
        
        # Draw a border around the main update window.
        
        update_window.box()
        
        # Update the internal window data structures.
        
        stdscr.noutrefresh()
        update_window.noutrefresh()
        
        # Redraw the screen.
        
        curses.doupdate()
        
        # Start the event loop.
        
        while True:

            c = update_window.getch()

            #####################################################################################

            # If user needs an update...
                
            if c == ord('u') or c == ord('U'):

                update_text_window.clear()
                update_text_window.addstr('Getting update...')
                update_text_window.refresh()
                update_text_window.clear()

                # Get updates from the hidden process log file.

                updates = reader('./{}.PIDS.log'.format(date))

                # Print updates to screen.

                try:
                    for line in updates:
                        update_text_window.addstr(line)
                        update_text_window.refresh()
                
                # If there are no updates to print, inform the user that there is a problem.

                except StopIteration:

                    update_text_window.addstr('Houston, we may have a problem.')
                    update_text_window.refresh()

            #####################################################################################

            # If user needs a summary...

            elif c == ord('s') or c == ord('S'):

                update_text_window.clear()
                update_text_window.addstr('Getting summary...')
                update_text_window.refresh()
                update_text_window.clear()
            
                # Restore information about the state of the pipeline.
                # Read first line to get configuration.

                summ_log = unpickler('./{}.history.log'.format(date))

                try:

                    # Get the configuration.

                    config = next(summ_log)

                    # Get the summary.

                    summary = summ_log

                    # Build the record.
 
                    record = build_record(config, summary)

                    # Print the summary on the screen.

                    for line in record:
                        update_text_window.addstr(line)
                        update_text_window.refresh()
                   
                    # Path where summary must be saved.

                    _rec_ = os.path.join(config.state_path,
                                         date,
                                         '{}.rec'.format(date))

                    # Save the recorded summary.

                    try:
                        with open(_rec_, 'w+') as _save_:
                            for line in record:
                                _save_.write(line)
                    except IOError:
                        update_text_window.addstr('\n')
                        update_text_window.addstr('Cannot write summary to file.')

                    # Inform the user that the summary has been saved
                    # and where it has been saved.

                    update_text_window.addstr('\n')
                    update_text_window.addstr('Summary saved at {}'.format(_rec_))

                    # Refresh window.           
         
                    update_text_window.refresh()

                except StopIteration:

                    # Hold your horses, user!

                    update_text_window.addstr('No files processed yet. Work ongoing.')
                    update_text_window.refresh()

            #####################################################################################
            
            elif c == ord('q') or c == ord('Q'):

                # Quit and exit event loop.

                break
        
        # Refresh the windows from the bottom up.
        
        stdscr.noutrefresh()
        update_window.noutrefresh()
        update_text_window.noutrefresh()
        curses.doupdate()

    #########################################################################

    # Wrap the "cli_updater" in the curses "wrapper" function so that all
    # initialisations are handled appropriately and the terminal is restored
    # to its former glory without a hitch, even if there is an exception.

    curses.wrapper(cli_updater, date)

    #########################################################################
