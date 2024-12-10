import requests
import json
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

username = 'laravel'
repository = 'framework'
token = 'your token'

# GitHub API URL
base_url = f'https://api.github.com/repos/{username}/{repository}/pulls'
params = {'state': 'closed', 'per_page': 100}
headers = {'Authorization': f'token {token}'}


def fetch_with_retry(url, params=None, headers=None, retries=3, delay=5):
    """带重试逻辑的 GET 请求"""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                print(f"Rate limit hit. Retrying after {delay} seconds... (Attempt {attempt + 1})")
                time.sleep(delay)
            else:
                raise e
    raise Exception(f"Failed to fetch data after {retries} retries: {url}")


def fetch_page(url, params, headers):
    """获取 PR 列表和下一页链接"""
    response = fetch_with_retry(url, params=params, headers=headers)
    return response.json(), response.links.get('next', {}).get('url')


def extract_linked_issues(pr_body):
    """从 PR 的 body 文本中提取关联的 Issue 编号"""
    if not pr_body:
        return []
    # 匹配 #123 格式的 Issue 引用
    return re.findall(r'#(\d+)', pr_body)


def fetch_issue_details(issue_number, headers):
    """获取关联 Issue 的详细信息，包括 title 和 description"""
    issue_url = f'https://api.github.com/repos/{username}/{repository}/issues/{issue_number}'
    issue_response = fetch_with_retry(issue_url, headers=headers)
    issue_data = issue_response.json()
    return {
        'id': issue_data['id'],
        'title': issue_data['title'],
        'description': issue_data['body']
    }


def parse_patch(patch):
    """解析 GitHub patch，提取所有新增和删除的行号及对应内容"""
    additions = []
    deletions = []
    addition_content = []
    deletion_content = []
    current_add_line = None
    current_del_line = None

    for line in patch.splitlines():
        if line.startswith('@@'):  # 行号信息
            match = re.search(r'@@ -(\d+),?\d* \+(\d+),?\d* @@', line)
            if match:
                current_del_line = int(match.group(1))
                current_add_line = int(match.group(2))
        elif line.startswith('+') and not line.startswith('+++'):  # 新增行
            additions.append(current_add_line)
            addition_content.append(line[1:])  # 去掉前面的 "+"
            current_add_line += 1
        elif line.startswith('-') and not line.startswith('---'):  # 删除行
            deletions.append(current_del_line)
            deletion_content.append(line[1:])  # 去掉前面的 "-"
            current_del_line += 1
        elif not line.startswith('+') and not line.startswith('-'):
            # 普通行，递增行号
            if current_add_line is not None:
                current_add_line += 1
            if current_del_line is not None:
                current_del_line += 1

    return {
        'additions_lines': additions,
        'deletions_lines': deletions,
        'addition_content': addition_content,
        'deletion_content': deletion_content
    }


def fetch_pr_details(pr_url, headers):
    """获取单个 PR 的详细信息，包括改动行数和关联 issue"""
    pr_response = fetch_with_retry(pr_url, headers=headers)
    pr_data = pr_response.json()

    # 从 PR body 提取关联 Issues
    associated_issues = extract_linked_issues(pr_data.get('body', ''))

    # 获取关联 Issue 的详细信息
    issues_details = []
    for issue_number in associated_issues:
        issue_details = fetch_issue_details(issue_number, headers)
        issues_details.append(issue_details)

    # 获取改动内容
    files_url = pr_data.get('url') + '/files'
    files_response = fetch_with_retry(files_url, headers=headers)
    files = files_response.json()

    changes = []
    for file in files:
        patch = file.get('patch', '')
        line_info = parse_patch(patch) if patch else {
            'additions_lines': [],
            'deletions_lines': [],
            'addition_content': [],
            'deletion_content': []
        }
        changes.append({
            'filename': file['filename'],
            'additions': file['additions'],
            'deletions': file['deletions'],
            'additions_lines': line_info['additions_lines'],
            'deletions_lines': line_info['deletions_lines'],
            'addition_content': line_info['addition_content'],
            'deletion_content': line_info['deletion_content']
        })

    return {
        'id': pr_data['id'],
        'title': pr_data['title'],
        'body': pr_data['body'],
        'created_at': pr_data['created_at'],
        'merged_at': pr_data['merged_at'],
        'associated_issues': issues_details,  # 保存每个关联 Issue 的详细信息
        'changes': changes
    }


def get_all_prs(base_url, params, headers, max_prs, processed_ids, output_file):
    """获取所有有效 PR 数据"""
    all_prs = []
    url = base_url

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        while url and len(all_prs) + len(processed_ids) < max_prs:
            prs, next_url = fetch_page(url, params, headers)
            url = next_url

            for pr in prs:
                if pr.get('merged_at') and pr['id'] not in processed_ids:  # 筛选合并且未处理的 PR
                    futures.append(executor.submit(fetch_pr_details, pr['url'], headers))

            for future in as_completed(futures):  # 异步处理任务结果
                try:
                    pr_data = future.result()
                    all_prs.append(pr_data)
                    
                    # 立即写入文件
                    with open(output_file, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(pr_data, ensure_ascii=False) + '\n')
                    
                    print(f"Fetched and saved PR: {len(all_prs)}/{max_prs - len(processed_ids)}")
                    if len(all_prs) + len(processed_ids) >= max_prs:
                        break
                except Exception as e:
                    print(f"Error fetching PR details: {e}")
            futures = []  # 清空任务列表

    return all_prs


if __name__ == "__main__":
    output_file = 'merged_prs.jsonl'

    # 读取已处理的 PR ID
    processed_ids = set()
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                processed_ids.add(data['id'])
    except FileNotFoundError:
        pass

    # 检查现有数据量
    if len(processed_ids) >= 2000:
        print("Already processed 2000 PRs. Exiting.")
    else:
        try:
            new_prs = get_all_prs(base_url, params, headers, max_prs=2000, processed_ids=processed_ids, output_file=output_file)
            print(f"Processed {len(new_prs)} new PRs.")
        except Exception as e:
            print(f"An error occurred: {e}")
