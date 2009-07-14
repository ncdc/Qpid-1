/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.management.ui.views.type;


import java.util.List;

import org.apache.qpid.management.ui.ManagedBean;
import org.apache.qpid.management.ui.jmx.MBeanUtility;
import org.apache.qpid.management.ui.views.MBeanView;
import org.apache.qpid.management.ui.views.NavigationView;
import org.apache.qpid.management.ui.views.TabControl;

import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.forms.widgets.Form;
import org.eclipse.ui.forms.widgets.FormToolkit;

public abstract class MBeanTypeTabControl extends TabControl
{
    private FormToolkit _toolkit;
    private Form        _form;
    protected Table _table = null;
    protected TableViewer _tableViewer = null;

    private List<ManagedBean> _mbeans = null;
    private String _type;
    
    
    public MBeanTypeTabControl(TabFolder tabFolder, String type)
    {
        super(tabFolder);
        _type = type;
        _toolkit = new FormToolkit(_tabFolder.getDisplay());
        _form = _toolkit.createForm(_tabFolder);
        _form.getBody().setLayout(new GridLayout());
        createWidgets();
    }

    /**
     * @see TabControl#getControl()
     */
    public Control getControl()
    {
        return _form;
    }
    
    /**
     * @see TabControl#setFocus()
     */
    public void setFocus()
    {
        _table.setFocus();
    }
    
    public void refresh()
    {
        refresh(null);
    }
    
    
    @Override
    public void refresh(ManagedBean mbean)
    {
        _mbeans = getMbeans();
        
        _tableViewer.setInput(_mbeans);

        layout();
    }
    
    public void layout()
    {
        _form.layout(true);
        _form.getBody().layout(true, true);
    }
    
    protected abstract List<ManagedBean> getMbeans();
    
    protected void createTable(Composite tableComposite)
    {
        _table = new Table (tableComposite, SWT.SINGLE | SWT.SCROLL_LINE | SWT.BORDER | SWT.FULL_SELECTION);
        _table.setLinesVisible (true);
        _table.setHeaderVisible (true);
        GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
        _table.setLayoutData(data);
        
        _tableViewer = new TableViewer(_table);
        final TableSorter tableSorter = new TableSorter();
        
        String[] titles = { "Name"};
        int[] bounds = { 310};
        for (int i = 0; i < titles.length; i++) 
        {
            final int index = i;
            final TableViewerColumn viewerColumn = new TableViewerColumn(_tableViewer, SWT.NONE);
            final TableColumn column = viewerColumn.getColumn();

            column.setText(titles[i]);
            column.setWidth(bounds[i]);
            column.setResizable(true);

            //Setting the right sorter
            column.addSelectionListener(new SelectionAdapter() 
            {
                @Override
                public void widgetSelected(SelectionEvent e) 
                {
                    tableSorter.setColumn(index);
                    final TableViewer viewer = _tableViewer;
                    int dir = viewer .getTable().getSortDirection();
                    if (viewer.getTable().getSortColumn() == column) 
                    {
                        dir = dir == SWT.UP ? SWT.DOWN : SWT.UP;
                    } 
                    else 
                    {
                        dir = SWT.UP;
                    }
                    viewer.getTable().setSortDirection(dir);
                    viewer.getTable().setSortColumn(column);
                    viewer.refresh();
                }
            });

        }
        
        _tableViewer.setContentProvider(new ContentProviderImpl());
        _tableViewer.setLabelProvider(new LabelProviderImpl());
        _tableViewer.setSorter(tableSorter);
        _table.setSortColumn(_table.getColumn(0));
        _table.setSortDirection(SWT.UP);
    }
    
    
    
    private void createWidgets()
    {
        Composite mainComposite = _toolkit.createComposite(_form.getBody(), SWT.NONE);
        mainComposite.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
        mainComposite.setLayout(new GridLayout());
        
        Composite buttonComposite = _toolkit.createComposite(mainComposite, SWT.NONE);
        GridData gridData = new GridData(SWT.FILL, SWT.FILL, true, false);
        buttonComposite.setLayoutData(gridData);
        buttonComposite.setLayout(new GridLayout(2,true));
        
        final Button favouritesButton = _toolkit.createButton(buttonComposite, 
                                                    "<-- Add " + _type + " to favourites", SWT.PUSH);
        gridData = new GridData(SWT.LEFT, SWT.CENTER, true, false);
        favouritesButton.setLayoutData(gridData);
        favouritesButton.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                int selectionIndex = _table.getSelectionIndex();

                if (selectionIndex != -1)
                {
                    final ManagedBean selectedMBean = (ManagedBean)_table.getItem(selectionIndex).getData();
                    
                    IWorkbenchWindow window = PlatformUI.getWorkbench().getActiveWorkbenchWindow(); 
                    NavigationView view = (NavigationView)window.getActivePage().findView(NavigationView.ID);
                    try
                    {
                        view.addManagedBean(selectedMBean);
                    }
                    catch (Exception ex)
                    {
                        MBeanUtility.handleException(selectedMBean, ex);
                    }

                }
            }
        });
        
        final Button openButton = _toolkit.createButton(buttonComposite, "Open selected " + _type, SWT.PUSH);
        gridData = new GridData(SWT.RIGHT, SWT.CENTER, true, false);
        openButton.setLayoutData(gridData);
        openButton.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                int selectionIndex = _table.getSelectionIndex();

                if (selectionIndex != -1)
                {
                    final ManagedBean selectedMBean = (ManagedBean)_table.getItem(selectionIndex).getData();
                    
                    IWorkbenchWindow window = PlatformUI.getWorkbench().getActiveWorkbenchWindow(); 
                    MBeanView view = (MBeanView) window.getActivePage().findView(MBeanView.ID);
                    try
                    {
                        view.openMBean(selectedMBean);
                    }
                    catch (Exception ex)
                    {
                        MBeanUtility.handleException(selectedMBean, ex);
                    }
                }
            }
        });
        
        Composite tableComposite = _toolkit.createComposite(mainComposite);
        gridData = new GridData(SWT.FILL, SWT.FILL, true, true);
        tableComposite.setLayoutData(gridData);
        tableComposite.setLayout(new GridLayout(1,false));
        
        createTable(tableComposite);
        
        favouritesButton.setEnabled(false);
        openButton.setEnabled(false);
        
        _tableViewer.addSelectionChangedListener(new ISelectionChangedListener(){
            public void selectionChanged(SelectionChangedEvent evt)
            {
                int selectionIndex = _table.getSelectionIndex();

                if (selectionIndex != -1)
                {
                    favouritesButton.setEnabled(true);
                    openButton.setEnabled(true);
                }
                else
                {
                    favouritesButton.setEnabled(false);
                    openButton.setEnabled(false);
                }
            }
        });
        
        _table.addMouseListener(new MouseListener()                                              
        {
            // MouseListener implementation
            public void mouseDoubleClick(MouseEvent event)
            {
                int selectionIndex = _table.getSelectionIndex();

                if (selectionIndex != -1)
                {
                    final ManagedBean selectedMBean = (ManagedBean)_table.getItem(selectionIndex).getData();
                    
                    IWorkbenchWindow window = PlatformUI.getWorkbench().getActiveWorkbenchWindow(); 
                    MBeanView view = (MBeanView) window.getActivePage().findView(MBeanView.ID);
                    try
                    {
                        view.openMBean(selectedMBean);
                    }
                    catch (Exception ex)
                    {
                        MBeanUtility.handleException(selectedMBean, ex);
                    }
                }
            }

            public void mouseDown(MouseEvent e){}
            public void mouseUp(MouseEvent e){}
        });
    }
    
    /**
     * Content Provider class for the table viewer
     */
    private class ContentProviderImpl  implements IStructuredContentProvider
    {
        
        public void inputChanged(Viewer v, Object oldInput, Object newInput)
        {
            
        }
        
        public void dispose()
        {
            
        }
        
        @SuppressWarnings("unchecked")
        public Object[] getElements(Object parent)
        {
            return ((List<ManagedBean>) parent).toArray();
        }
    }
    
    /**
     * Label Provider class for the table viewer
     */
    private class LabelProviderImpl extends LabelProvider implements ITableLabelProvider
    {
        @Override
        public String getColumnText(Object element, int columnIndex)
        {
            switch (columnIndex)
            {
                case 0 : // name column 
                    return ((ManagedBean) element).getName();
                default:
                    return "-";
            }
        }
        
        @Override
        public Image getColumnImage(Object element, int columnIndex)
        {
            return null;
        }
        
    }

    /**
     * Sorter class for the table viewer.
     *
     */
    private class TableSorter extends ViewerSorter
    {
        private int column;
        private static final int ASCENDING = 0;
        private static final int DESCENDING = 1;

        private int direction;

        public TableSorter()
        {
            this.column = 0;
            direction = ASCENDING;
        }

        public void setColumn(int column)
        {
            if(column == this.column)
            {
                // Same column as last sort; toggle the direction
                direction = 1 - direction;
            }
            else
            {
                // New column; do an ascending sort
                this.column = column;
                direction = ASCENDING;
            }
        }

        @Override
        public int compare(Viewer viewer, Object e1, Object e2)
        {
            ManagedBean mbean1 = (ManagedBean) e1;
            ManagedBean mbean2 = (ManagedBean) e2;
            
            int comparison = 0;
            switch(column)
            {
                case 0:
                    comparison = mbean1.getName().compareTo(mbean2.getName());
                    break;
                default:
                    comparison = 0;
            }
            // If descending order, flip the direction
            if(direction == DESCENDING)
            {
                comparison = -comparison;
            }
            return comparison;
        }
    }
}
