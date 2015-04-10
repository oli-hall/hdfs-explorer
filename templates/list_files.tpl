<html>
    <head>
        <title>Files for HDFS:///{{path}}</title>
    </head>
    <body>
        <h3>Results</h3>
        <ul>
        % for file in files:
            % if file['is_dir']:
                <li>
                    dir: <a href='/v1/files{{file['path']}}'>{{file['name']}}</a>
                    % if 'children' in file:
                    <ul>
                    % for ch in file['children']:
                        <li>
                            <a href='/v1/files{{ch['path']}}'>{{ch['name']}}</a>
                            % if 'children' in ch:
                            <ul>
                            % for gch in ch['children']:
                                <li>
                                    <a href='/v1/files{{gch['path']}}'>{{gch['name']}}</a>
                                </li>
                            % end
                            </ul>
                            % end
                        </li>
                    % end
                    </ul>
                    % end
                </li>
            % else:
                <li>file: <a href='/v1/files{{file['path']}}'>{{file['name']}}</a></li>
            % end
        % end
        </ul>
    </body>
</html>
