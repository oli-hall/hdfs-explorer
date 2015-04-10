<html>
    <head>
        <title>Files for HDFS:///{{path}}</title>
    </head>
    <body>
        <h3>Results</h3>
        <ul>
        % for file in files:
            % if file['is_dir']:
                <li>dir: <a href='/v1/files{{file['path']}}'>{{file['name']}}</a></li>
            % else:
                <li>file: <a href='/v1/files{{file['path']}}'>{{file['name']}}</a></li>
            % end
        % end
        </ul>
    </body>
</html>
