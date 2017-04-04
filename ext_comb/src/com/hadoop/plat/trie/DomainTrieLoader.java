package com.hadoop.plat.trie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import com.hadoop.plat.util.EnvUtil;

public class DomainTrieLoader {

	private String whiteListFile = "./categoryCode.txt";
	private String blackListFile = "./blackList.txt";

	public DomainTrieLoader() {
	}

	public DomainTrieLoader(String categoryCodeFileName, String blacklistFileName) {
		if (categoryCodeFileName == null) {
			throw new IllegalArgumentException("categoryCodeFile can not be null.");
		}
		this.whiteListFile = categoryCodeFileName;
		this.blackListFile = blacklistFileName;
	}

	/**
	 * 加载配置文件，并构造Trie树。
	 * 
	 * @return
	 * @throws Exception
	 */
	public DomainTrie<DomainPattern> load() throws Exception {
		File ccf = new File(EnvUtil.getPath(this.whiteListFile));
		if (!ccf.exists()) {
			throw new IllegalArgumentException(whiteListFile + " does not exists");
		}

		DomainTrie<DomainPattern> trie = new DomainTrie<>();
		// 加载白名单
		try (BufferedReader br = openFile(ccf)) {
			handleCategoryFile(br, trie);
		}
		if (this.blackListFile == null) {
			return trie;
		}
		// 加载黑名单
		File blf = new File(EnvUtil.getPath(this.blackListFile));
		if (!blf.exists()) {
			throw new IllegalArgumentException(blackListFile + " doesnot exists");
		}
		try (BufferedReader br = openFile(blf)) {
			handleBlackFile(br, trie);
		}
		return trie;
	}

	private BufferedReader openFile(File file) throws UnsupportedEncodingException, FileNotFoundException {
		return new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
	}

	private void handleCategoryFile(BufferedReader br, DomainTrie<DomainPattern> trie) throws IOException {
		DomainPattern pattern = null;
		for (String line; ((line = br.readLine()) != null);) {
			pattern = DomainPattern.parseWhiteList(line);
			if (pattern == null) {
				continue;
			}
			trie.addDomain(pattern);
		}
	}

	private void handleBlackFile(BufferedReader br, DomainTrie<DomainPattern> trie) throws IOException {
		DomainPattern pattern = null;
		for (String line; ((line = br.readLine()) != null);) {
			pattern = DomainPattern.parseBlackList(line);
			if (pattern == null) {
				continue;
			}
			trie.addDomain(pattern);
		}
	}
}
